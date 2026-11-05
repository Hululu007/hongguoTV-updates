// SPDX-License-Identifier: GPL-3.0-only
package com.hongguotv.core

import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.IOException
import java.io.InputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.net.SocketTimeoutException
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * 手机遥控的局域网服务。
 *
 * 和 [PhoneInputServer] 的关键区别：那边是「一次请求-响应即关闭」，这里是长连接，
 * 所以不能有 `consumed` 一次性原子位，也不能有绝对 TTL——只用空闲回收。
 *
 * 线程模型：
 * - accept 线程 1 条，`soTimeout=500` 轮询，顺带做空闲检查；
 * - SSE 连接 1 条（单槽位），外加 1 条看门狗；
 * - POST /cmd 由 accept 线程内的临时线程处理，处理完立即退出。
 *
 * [post] 由调用方提供「切到主线程」的实现（Android 是 `Handler::post`，
 * 单测直接传 `{ it.run() }`），core 不依赖 Android。
 */
class LanRemoteHub(
    address: String,
    private val post: (Runnable) -> Unit,
    private val target: () -> RemoteTarget?,
    private val controller: () -> RemoteController?,
    private val resumeItems: () -> List<ResumeItem> = { emptyList() },
    private val onExpired: () -> Unit = {},
    private val idleTimeoutMs: Long = 30 * 60_000,
    private val heartbeatMs: Long = 15_000,
    private val watchdogMs: Long = 40_000,
    private val snapshotIntervalMs: Long = 400,
    private val cmdRateLimit: Int = 20
) : Closeable {
    private val stopped = AtomicBoolean(false)
    private val localAddress = InetAddress.getByName(address).also {
        require(it.isLoopbackAddress || it.isSiteLocalAddress) { "遥控只监听内网地址" }
    }.hostAddress
    private val listener = ServerSocket(0, 8).apply { soTimeout = 500 }
    private val token = ByteArray(16).also { java.security.SecureRandom().nextBytes(it) }
        .joinToString("") { "%02x".format(it.toInt() and 255) }
    private val host = "$localAddress:${listener.localPort}"
    val url: String = "http://$host/$token"
    val address: String = localAddress
    val port: Int get() = listener.localPort

    /** 主线程写、socket 线程读。绝不能在 socket 线程直接碰播放器。 */
    @Volatile var snapshot: PlaybackSnapshot = PlaybackSnapshot()

    /** Activity 重建时自增，手机端据此判断是否要重拉全量。 */
    @Volatile var epoch: Long = 1L
        private set

    @Volatile private var sse: SseClient? = null
    @Volatile private var lastActivity = System.nanoTime()
    private val seqCounter = AtomicLong()
    private val lastCommandSeq = AtomicLong()
    private val pendingLock = Any()
    private var pendingIntent: RemoteCommand? = null
    private val rateLock = Any()
    private var rateWindowStart = 0L
    private var rateCount = 0
    private var evictions = 0
    private var evictionWindowStart = 0L

    private val acceptThread = Thread({ serve() }, "hongguotv-remote").apply { isDaemon = true }

    init { acceptThread.start() }

    val clientCount: Int get() = if(sse != null) 1 else 0

    override fun close() {
        stopped.set(true)
        dropClient(notify = true)
        runCatching { listener.close() }
    }

    fun bumpEpoch() { epoch++ }

    /** 取走断线/后台期间缓存的最后一条幂等意图；只有一条，绝不回放队列。 */
    fun takePending(): RemoteCommand? = synchronized(pendingLock) {
        val value = pendingIntent
        pendingIntent = null
        value
    }

    // ---------------------------------------------------------------- 监听

    private fun serve() {
        try {
            while(!stopped.get()) {
                val now = System.nanoTime()
                if(sse == null && now - lastActivity > idleTimeoutMs * 1_000_000) {
                    stop("空闲回收")
                    return
                }
                val client = try { listener.accept() } catch(_: SocketTimeoutException) { continue }
                Thread({ handle(client) }, "hongguotv-remote-req").apply { isDaemon = true }.start()
            }
        } catch(_: Exception) {
            // close() 打断 accept()
        } finally {
            if(!stopped.get()) stop("监听结束")
        }
    }

    private fun stop(reason: String) {
        if(!stopped.compareAndSet(false, true)) return
        dropClient(notify = true)
        runCatching { listener.close() }
        post { controller()?.onSessionEnded() }
        onExpired()
    }

    // ---------------------------------------------------------------- HTTP

    private fun handle(socket: Socket) {
        socket.use {
            try {
                if(!socket.inetAddress.let { a -> a.isLoopbackAddress || a.isSiteLocalAddress }) return
                lastActivity = System.nanoTime()
                socket.soTimeout = 4000
                route(socket)
            } catch(_: SocketTimeoutException) {
                reply(socket, 408, "请求超时")
            } catch(_: Exception) {
                // 客户端主动断开、畸形请求：丢弃即可，不断服务
            }
        }
    }

    private fun route(socket: Socket) {
        val input = socket.getInputStream().buffered()
        val line = readLine(input, 1024)
        val parts = line.split(' ')
        if(parts.size != 3) return reply(socket, 400, "请求格式错误")
        val method = parts[0]
        val path = parts[1].substringBefore('?')
        val headers = readHeaders(input) ?: return reply(socket, 431, "请求过大")
        if(headers["host"] != host) return reply(socket, 404, "页面不存在")
        if(!path.startsWith("/$token")) return reply(socket, 404, "页面不存在")
        val route = path.removePrefix("/$token").ifEmpty { "/" }
        when {
            method == "GET" && route == "/" -> reply(socket, 200, LanRemotePage.html, "text/html")
            method == "GET" && route == "/events" -> serveEvents(socket, headers)
            method == "GET" && route == "/episodes" -> {
                allowOrigin(socket, headers) ?: return
                reply(socket, 200, episodesOnMain().toEpisodesJson(), "application/json")
            }
            method == "GET" && route == "/resume" -> {
                allowOrigin(socket, headers) ?: return
                reply(socket, 200, resumeOnMain().toResumeJson(), "application/json")
            }
            method == "POST" && route == "/cmd" -> handleCommand(socket, input, headers)
            else -> reply(socket, 405, "不支持的操作")
        }
    }

    /** Origin 是纵深防御，token 才是真门禁。SSE 的 GET 也必须校验，否则等于开了个裸读口。 */
    private fun allowOrigin(socket: Socket, headers: Map<String, String>): Unit? {
        if(foreignOrigin(headers["origin"], localAddress)) {
            reply(socket, 403, "来源不匹配")
            return null
        }
        return Unit
    }

    private fun handleCommand(socket: Socket, input: InputStream, headers: Map<String, String>) {
        allowOrigin(socket, headers) ?: return
        if(headers.containsKey("transfer-encoding")) return reply(socket, 400, "请求格式错误")
        val length = headers["content-length"]?.toIntOrNull()?.takeIf { it in 1..RemoteProtocol.MAX_BODY }
            ?: return reply(socket, 413, "命令过长")
        if(!allowRate()) return reply(socket, 429, "操作过于频繁")
        val body = ByteArray(length)
        var offset = 0
        while(offset < body.size) {
            val read = input.read(body, offset, body.size - offset)
            if(read < 0) return
            offset += read
        }
        val text = String(body, Charsets.UTF_8)
        val command = RemoteProtocol.parse(text) ?: return reply(socket, 400, "命令无效")
        if(command is RemoteCommand.Detach) {
            dropClient(notify = true)
            return reply(socket, 200, "{\"ok\":true}")
        }
        val seq = RemoteProtocol.seqOf(text)
        if(seq != null && seq <= lastCommandSeq.get()) return reply(socket, 409, "命令已过期")
        if(seq != null) lastCommandSeq.set(seq)
        dispatch(command)
        reply(socket, 200, """{"ok":true${if(seq != null) ",\"seq\":$seq" else ""}}""", "application/json")
    }

    /** 单槽位，所以限速就是全局的；按 IP 分桶在 maxConnections=1 下没有区别。 */
    private fun allowRate(): Boolean = synchronized(rateLock) {
        val now = System.nanoTime()
        if(now - rateWindowStart >= 1_000_000_000L) { rateWindowStart = now; rateCount = 0 }
        ++rateCount <= cmdRateLimit
    }

    // ---------------------------------------------------------------- 命令

    private fun dispatch(command: RemoteCommand) {
        post {
            val accepted = if(command.isPlayback) target()?.apply(command) == true
            else controller()?.navigate(command) == true
            if(accepted) synchronized(pendingLock) { pendingIntent = null }
            else if(command.durable) synchronized(pendingLock) { pendingIntent = command }
        }
    }

    private fun <T> onMain(fallback: T, block: () -> T): T {
        var result: T? = null
        val latch = java.util.concurrent.CountDownLatch(1)
        post { result = block(); latch.countDown() }
        latch.await(1500, java.util.concurrent.TimeUnit.MILLISECONDS)
        return result ?: fallback
    }

    private fun episodesOnMain(): List<EpisodeGroupDetail> = onMain(emptyList()) { target()?.episodes() ?: emptyList() }
    private fun resumeOnMain(): List<ResumeItem> = onMain(emptyList()) { resumeItems() }

    // ---------------------------------------------------------------- SSE

    private inner class SseClient(val socket: Socket) {
        val lock = Any()
        @Volatile var lastWriteAt = System.nanoTime()
        /** 循环该退出了，但 socket 还开着——顶替时还要往里写一条 evicted。 */
        @Volatile var stopRequested = false
        @Volatile var closed = false
        fun close() { stopRequested = true; closed = true; runCatching { socket.close() } }
    }

    private fun serveEvents(socket: Socket, headers: Map<String, String>) {
        if(foreignOrigin(headers["origin"], localAddress)) return reply(socket, 403, "来源不匹配")
        socket.soTimeout = 0
        val client = SseClient(socket)
        val previous = sse
        sse = client
        if(previous != null && previous !== client) evict(previous)
        notifyClients()

        val watchdog = Thread({ watch(client) }, "hongguotv-remote-wdt").apply { isDaemon = true }
        watchdog.start()
        try {
            write(client, "retry: 1500\n\n")
            var sentAt = 0L
            var lastJson = ""
            var structural = ""
            while(!stopped.get() && !client.stopRequested) {
                val snap = snapshot
                val mark = structuralOf(snap)
                val now = System.nanoTime()
                val uri = renderSnapshot()
                val changed = uri != lastJson
                if(changed && (mark != structural || now - sentAt >= snapshotIntervalMs * 1_000_000)) {
                    write(client, "event: snap\ndata: $uri\n\n")
                    lastJson = uri
                    structural = mark
                    sentAt = now
                } else if(now - client.lastWriteAt >= heartbeatMs * 1_000_000) {
                    write(client, ":hb\n\n")
                }
                Thread.sleep(120)
            }
        } catch(_: Exception) {
            // 断线、看门狗 close、服务停止都走这里
        } finally {
            client.close()
            if(sse === client) {
                sse = null
                notifyClients()
            }
        }
    }

    /** 位置以外的一切。结构变化立即下发，只有位置变化才走 400ms 节流。 */
    private fun structuralOf(snap: PlaybackSnapshot): String = buildString {
        append(snap.source).append('|').append(snap.title).append('|').append(snap.group).append('|')
        append(snap.episode).append('|').append(snap.playing).append('|').append(snap.buffering).append('|')
        append(snap.speed).append('|').append(snap.muted).append('|').append(snap.danmaku).append('|')
        append(snap.error).append('|').append(snap.foreground).append('|').append(snap.screen).append('|')
        append(snap.durationMs).append('|').append(epoch)
    }

    private fun renderSnapshot(): String =
        snapshot.toJson(epoch, seqCounter.get(), System.currentTimeMillis(), clientCount, evictions)

    private fun write(client: SseClient, text: String) {
        synchronized(client.lock) {
            if(client.closed) throw SocketException("closed")
            client.socket.getOutputStream().apply {
                write(text.toByteArray(Charsets.UTF_8))
                flush()
            }
            client.lastWriteAt = System.nanoTime()
        }
    }

    /**
     * 写看门狗。Java 没有 SO_SNDTIMEO，TCP 半开时 write() 会永久阻塞，
     * 只能靠「多久没成功写入」来判断并从另一条线程 close 掉，让阻塞的 write 抛异常退出。
     * 阈值必须明显大于心跳间隔，否则健康连接会在两次心跳之间被误杀。
     */
    private fun watch(client: SseClient) {
        while(!client.stopRequested && !stopped.get()) {
            try { Thread.sleep(3_000) } catch(_: InterruptedException) { return }
            if(System.nanoTime() - client.lastWriteAt > watchdogMs * 1_000_000) {
                client.close()
                return
            }
        }
    }

    /**
     * 顶替旧连接。不能在当前线程里直接写：旧连接可能正卡在 write 上并持有它的锁，
     * 等锁会把新连接的握手一起拖住。所以先置 closed 让旧循环停下，再丢到后台线程尽力通知。
     */
    private fun evict(client: SseClient) {
        client.stopRequested = true
        Thread({
            val body = """{"reason":"另一个设备接管了遥控"}"""
            runCatching { write(client, "event: evicted\ndata: $body\n\n") }
            client.close()
        }, "hongguotv-remote-evict").apply { isDaemon = true }.start()
        claimEviction()
    }

    private fun notifyClients() {
        val count = clientCount
        post { controller()?.onClientsChanged(count) }
    }

    private fun dropClient(notify: Boolean) {
        val current = sse ?: return
        sse = null
        current.close()
        if(notify) notifyClients()
    }

    /** 60 秒内被顶替超过 3 次说明有客户端在互踢，直接销毁会话要求重扫。 */
    private fun claimEviction() {
        val now = System.nanoTime()
        synchronized(rateLock) {
            if(now - evictionWindowStart > 60_000_000_000L) { evictionWindowStart = now; evictions = 0 }
            evictions++
        }
        if(evictions > 3) stop("反复顶替")
    }

    // ---------------------------------------------------------------- 报文

    private fun readLine(input: InputStream, max: Int): String {
        val out = ByteArrayOutputStream()
        while(out.size() < max) {
            val next = input.read()
            if(next < 0) throw IOException("unexpected eof")
            if(next == 10) return out.toString("US-ASCII").trimEnd('\r')
            out.write(next)
        }
        throw IOException("header too large")
    }

    private fun readHeaders(input: InputStream): Map<String, String>? {
        val headers = mutableMapOf<String, String>()
        var bytes = 0
        while(true) {
            val line = readLine(input, 4096)
            bytes += line.length + 2
            if(bytes > 8192) return null
            if(line.isEmpty()) return headers
            val at = line.indexOf(':')
            if(at <= 0) return null
            val key = line.substring(0, at).lowercase(Locale.ROOT)
            if(headers.put(key, line.substring(at + 1).trim()) != null) return null
        }
    }

    private fun reply(socket: Socket, code: Int, body: String, contentType: String = "text/plain; charset=utf-8") {
        val payload = body.toByteArray(Charsets.UTF_8)
        val head = buildString {
            append("HTTP/1.1 ").append(code).append(' ').append(statusText(code)).append("\r\n")
            append("Content-Type: ").append(contentType).append(if(contentType.contains("charset")) "" else "; charset=utf-8").append("\r\n")
            append("Content-Length: ").append(payload.size).append("\r\n")
            append("Connection: close\r\n")
            append("Cache-Control: no-store\r\n")
            append("Referrer-Policy: no-referrer\r\n")
            append("X-Content-Type-Options: nosniff\r\n")
            append("Content-Security-Policy: ").append(CSP).append("\r\n\r\n")
        }
        runCatching {
            socket.getOutputStream().apply { write(head.toByteArray(Charsets.US_ASCII)); write(payload); flush() }
        }
    }

    private fun statusText(code: Int): String = when(code) {
        200 -> "OK"; 400 -> "Bad Request"; 403 -> "Forbidden"; 404 -> "Not Found"
        405 -> "Method Not Allowed"; 408 -> "Request Timeout"; 409 -> "Conflict"
        413 -> "Payload Too Large"; 429 -> "Too Many Requests"; 431 -> "Request Header Fields Too Large"
        else -> "Error"
    }

    companion object {
        /**
         * 与手机填写页不同，这里必须放开 script-src 与 connect-src：
         * EventSource 受 connect-src 约束，漏了会静默失败且没有任何报错。
         */
        const val CSP = "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; form-action 'none'; frame-ancestors 'none'; base-uri 'none'"
    }
}

pluginManagement { repositories { google(); mavenCentral(); gradlePluginPortal() } }
dependencyResolutionManagement { repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS); repositories { google(); mavenCentral() } }
rootProject.name = "HongguoTVNative"
include(":core", ":app")
// app-phone 鏄湰鍦板疄楠屾ā鍧楋紝姘镐笉鍏ュ簱锛堣 .gitignore:42锛夈€?// 鐢ㄦ潯浠跺寘鍚繚鎸?settings 涓庝粨搴撳唴瀹逛竴鑷达細鏈湴鏈夌洰褰曟墠鍙備笌鏋勫缓锛孋I / 鏂板厠闅嗚嚜鍔ㄨ烦杩囥€?if (file("app-phone").exists()) include(":app-phone")
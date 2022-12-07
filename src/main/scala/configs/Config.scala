package configs

case class Config(
                   clientId: Long = 0,
                   action: String = "",
                   hdfsIp: String = "",
                   hdfsPath: String = "",
                   fileName: String = "")

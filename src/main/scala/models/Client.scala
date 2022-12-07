package models

import java.sql.Timestamp

case class Client(clientId: Long, lastName: String, firstName: String, address: String, subscriptionDate: Timestamp)

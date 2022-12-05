package models

import java.time.LocalDateTime

case class Client(IdentifiantClient: Long, Nom: String, Prenom: String, Adresse: String, DateDeSouscription: LocalDateTime)

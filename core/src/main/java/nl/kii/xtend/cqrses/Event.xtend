package nl.kii.xtend.cqrses

import java.io.Serializable
import java.util.UUID

interface Event extends Serializable {
	
	def UUID getId()

}
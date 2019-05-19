package edu.ateneo.nrg.spark

import scala.io.StdIn.readLine
import scala.io.Source

object Utils {

    def configTwitterCredentials(pathToCredentialsFile: String) = {
        for (line <- Source.fromFile(pathToCredentialsFile).getLines) {
            val credential = line.split(" ")
            System.setProperty("twitter4j.oauth." + credential(0), credential(1))
        }
    }
}
package cakal.lab

import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class CoinReceiver(url: String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart(): Unit = {
    new Thread("Http Receiver") {
      override def run(): Unit = { receive() }
    }.start()
  }

  def onStop(): Unit = {
  }

  private def receive(): Unit = {
    var userInput: String = null
    var httpClient: DefaultHttpClient = null
    var getRequest: HttpGet = null

    try {
      httpClient = new DefaultHttpClient()
      getRequest = new HttpGet(url)
      getRequest.addHeader("accept", "application/json")

      while(!isStopped) {
        val response = httpClient.execute(getRequest)
        if (response.getStatusLine.getStatusCode != 200) {
          throw new RuntimeException("Failed : HTTP error code : "+ response.getStatusLine.getStatusCode)
        }
        val reader = new BufferedReader(new InputStreamReader(response.getEntity.getContent))
        userInput = reader.readLine()
        while(userInput != null) {
          store(userInput)
          userInput = reader.readLine()
        }
        reader.close()
        Thread.sleep(60*1000)
      }
    } catch {
      case e: java.net.ConnectException =>
        restart("Error connecting to " + url, e)
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }
}
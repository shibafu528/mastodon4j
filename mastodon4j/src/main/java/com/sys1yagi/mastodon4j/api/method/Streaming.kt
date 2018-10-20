package com.sys1yagi.mastodon4j.api.method

import com.sys1yagi.mastodon4j.MastodonClient
import com.sys1yagi.mastodon4j.Parameter
import com.sys1yagi.mastodon4j.api.Dispatcher
import com.sys1yagi.mastodon4j.api.Handler
import com.sys1yagi.mastodon4j.api.Shutdownable
import com.sys1yagi.mastodon4j.api.entity.Notification
import com.sys1yagi.mastodon4j.api.entity.Status
import com.sys1yagi.mastodon4j.api.exception.Mastodon4jRequestException
import java.io.BufferedReader

class Streaming(private val client: MastodonClient) {
    private inline fun <reified T> handleAnything(event: String, payload: String, client: MastodonClient, expected: String, receiver: (T) -> Unit): Boolean {
        return if (event == expected) {
            val start = payload.indexOf(":") + 1
            val json = payload.substring(start).trim()
            val any = client.getSerializer().fromJson(
                    json,
                    T::class.java
            )
            receiver(any)
            true
        } else {
            false
        }
    }

    private inline fun handleStatus(event: String, payload: String, client: MastodonClient, receiver: (Status) -> Unit): Boolean {
        return handleAnything<Status>(event, payload, client, "update", receiver)
    }

    private inline fun handleNotification(event: String, payload: String, client: MastodonClient, receiver: (Notification) -> Unit): Boolean {
        return handleAnything<Notification>(event, payload, client, "notification", receiver)
    }

    private inline fun handleDelete(event: String, payload: String, client: MastodonClient, receiver: (Long) -> Unit): Boolean {
        return handleAnything<Long>(event, payload, client, "delete", receiver)
    }

    private fun read(reader: BufferedReader): Pair<String, String>? {
        val line = reader.readLine()?.takeIf { it.isNotEmpty() } ?: return null
        val type = line.split(":")[0].trim()
        if (type != "event") {
            return null
        }
        val payload = reader.readLine()
        return Pair(line, payload)
    }

    private inline fun receiveStatus(reader: BufferedReader, client: MastodonClient, receiver: (Status) -> Unit) {
        val (line, payload) = read(reader) ?: return
        val payloadType = payload.split(":")[0].trim()
        if (payloadType != "data") {
            return
        }
        val event = line.split(":")[1].trim()
        handleStatus(event, payload, client, receiver)
    }

    private fun receiveUser(reader: BufferedReader, client: MastodonClient, handler: Handler) {
        val (line, payload) = read(reader) ?: return
        val payloadType = payload.split(":")[0].trim()
        if (payloadType != "data") {
            return
        }
        val event = line.split(":")[1].trim()
        handleStatus(event, payload, client, handler::onStatus)
                || handleNotification(event, payload, client, handler::onNotification)
                || handleDelete(event, payload, client, handler::onDelete)
    }

    @Throws(Mastodon4jRequestException::class)
    fun federatedPublic(handler: Handler): Shutdownable {
        val response = client.get("streaming/public")
        if (response.isSuccessful) {
            val reader = response.body().byteStream().bufferedReader()
            val dispatcher = Dispatcher()
            dispatcher.invokeLater(Runnable {
                while (true) {
                    try{
                        receiveStatus(reader, client, handler::onStatus)
                    }catch (e:java.io.InterruptedIOException){
                        break
                    }
                }
                try {
                    reader.close()
                } catch (ignore: java.io.IOException) {}
            })
            return Shutdownable(dispatcher)
        } else {
            throw Mastodon4jRequestException(response)
        }
    }

    @Throws(Mastodon4jRequestException::class)
    fun localPublic(handler: Handler): Shutdownable {
        val response = client.get("streaming/public/local")
        if (response.isSuccessful) {
            val reader = response.body().byteStream().bufferedReader()
            val dispatcher = Dispatcher()
            dispatcher.invokeLater(Runnable {
                while (true) {
                    try{
                        receiveStatus(reader, client, handler::onStatus)
                    }catch (e:java.io.InterruptedIOException){
                        break
                    }
                }
                try {
                    reader.close()
                } catch (ignore: java.io.IOException) {}
            })
            return Shutdownable(dispatcher)
        } else {
            throw Mastodon4jRequestException(response)
        }
    }

    @Throws(Mastodon4jRequestException::class)
    fun federatedHashtag(tag: String, handler: Handler): Shutdownable {
        val response = client.get(
                "streaming/hashtag",
                Parameter().append("tag", tag)
        )
        if (response.isSuccessful) {
            val reader = response.body().byteStream().bufferedReader()
            val dispatcher = Dispatcher()
            dispatcher.invokeLater(Runnable {
                while (true) {
                    try{
                        receiveStatus(reader, client, handler::onStatus)
                    }catch (e:java.io.InterruptedIOException){
                        break
                    }
                }
                try {
                    reader.close()
                } catch (ignore: java.io.IOException) {}
            })
            return Shutdownable(dispatcher)
        } else {
            throw Mastodon4jRequestException(response)
        }
    }

    @Throws(Mastodon4jRequestException::class)
    fun localHashtag(tag: String, handler: Handler): Shutdownable {
        val response = client.get(
                "streaming/hashtag/local",
                Parameter().append("tag", tag)
        )
        if (response.isSuccessful) {
            val reader = response.body().byteStream().bufferedReader()
            val dispatcher = Dispatcher()
            dispatcher.invokeLater(Runnable {
                while (true) {
                    try{
                        receiveStatus(reader, client, handler::onStatus)
                    }catch (e:java.io.InterruptedIOException){
                        break
                    }
                }
                try {
                    reader.close()
                } catch (ignore: java.io.IOException) {}
            })
            return Shutdownable(dispatcher)
        } else {
            throw Mastodon4jRequestException(response)
        }
    }

    @Throws(Mastodon4jRequestException::class)
    fun user(handler: Handler): Shutdownable {
        val response = client.get(
                "streaming/user"
        )
        if (response.isSuccessful) {
            val reader = response.body().byteStream().bufferedReader()
            val dispatcher = Dispatcher()
            dispatcher.invokeLater(Runnable {
                while (true) {
                    try{
                        receiveUser(reader, client, handler)
                    }catch (e:java.io.InterruptedIOException){
                        break
                    }
                }
                try {
                    reader.close()
                } catch (ignore: java.io.IOException) {}
            })
            return Shutdownable(dispatcher)
        } else {
            throw Mastodon4jRequestException(response)
        }
    }

    @Throws(Mastodon4jRequestException::class)
    fun userList(handler: Handler, listID: String): Shutdownable {
        val response = client.get(
                "streaming/list",
                Parameter().apply {
                    append("list", listID)
                }
        )
        if (response.isSuccessful) {
            val reader = response.body().byteStream().bufferedReader()
            val dispatcher = Dispatcher()
            dispatcher.invokeLater(Runnable {
                while (true) {
                    try{
                        receiveUser(reader, client, handler)
                    }catch (e:java.io.InterruptedIOException){
                        break
                    }
                }
                reader.close()
            })
            return Shutdownable(dispatcher)
        } else {
            throw Mastodon4jRequestException(response)
        }
    }
}

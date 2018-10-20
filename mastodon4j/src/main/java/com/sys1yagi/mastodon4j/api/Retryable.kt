package com.sys1yagi.mastodon4j.api

import com.sys1yagi.mastodon4j.api.exception.Mastodon4jRequestException

interface Retryable {
    @Throws(Mastodon4jRequestException::class)
    fun retry()
}
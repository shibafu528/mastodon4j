package com.sys1yagi.mastodon4j.api.method

import com.sys1yagi.mastodon4j.MastodonClient
import com.sys1yagi.mastodon4j.api.Pageable
import com.sys1yagi.mastodon4j.api.Range
import com.sys1yagi.mastodon4j.api.entity.Account
import com.sys1yagi.mastodon4j.api.exception.Mastodon4jRequestException
import com.sys1yagi.mastodon4j.extension.emptyRequestBody
import com.sys1yagi.mastodon4j.extension.genericType
import com.sys1yagi.mastodon4j.extension.toPageable

/**
 * See more https://github.com/tootsuite/documentation/blob/master/Using-the-API/API.md#follow-requests
 */
class FollowRequests(private  val client: MastodonClient) {
    // GET /api/v1/follow_requests
    @JvmOverloads
    @Throws(Mastodon4jRequestException::class)
    fun getFollowRequests(range: Range = Range()): Pageable<Account> {
        val response = client.get("follow_requests", range.toParameter())
        if (response.isSuccessful) {
            val body = response.body().string()
            return client.getSerializer().fromJson<List<Account>>(
                    body,
                    genericType<List<Account>>()
            ).toPageable(response)
        } else {
            throw Mastodon4jRequestException(response)
        }
    }

    //  POST /api/v1/follow_requests/:id/authorize
    @Throws(Mastodon4jRequestException::class)
    fun postAuthorize(accountId: Long) {
        val response = client.post("follow_requests/$accountId/authorize", emptyRequestBody())
        if (!response.isSuccessful) {
            throw Mastodon4jRequestException(response)
        }
    }

    //  POST /api/v1/follow_requests/:id/reject
    @Throws(Mastodon4jRequestException::class)
    fun postReject(accountId: Long) {
        val response = client.post("follow_requests/$accountId/reject", emptyRequestBody())
        if (!response.isSuccessful) {
            throw Mastodon4jRequestException(response)
        }
    }
}

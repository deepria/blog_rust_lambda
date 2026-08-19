use crate::api::{ApiError, AppResult};
use crate::domain::auth::{self, User};
use crate::dynamodb::{
    get_json, query_json_prefix, transact_write_values, TransactionDelete, TransactionPut,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const FRIEND_PART: &str = "FRIEND";
const FRIEND_REQUEST_PART: &str = "FRIEND_REQUEST";
const FRIEND_REQUEST_PAIR_PART: &str = "FRIEND_REQUEST_PAIR";

#[derive(Debug, Deserialize)]
pub struct CreateFriendRequest {
    pub email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FriendRequestRecord {
    request_id: String,
    sender_id: String,
    sender_name: String,
    sender_email: String,
    sender_avatar_url: Option<String>,
    recipient_id: String,
    recipient_name: String,
    recipient_email: String,
    recipient_avatar_url: Option<String>,
    created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FriendRecord {
    user_id: String,
    friend_id: String,
    friend_name: String,
    friend_email: String,
    friend_avatar_url: Option<String>,
    friends_since: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FriendItem {
    pub user_id: String,
    pub name: String,
    pub email: String,
    pub avatar_url: Option<String>,
    pub friends_since: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FriendRequestItem {
    pub request_id: String,
    pub user_id: String,
    pub name: String,
    pub email: String,
    pub avatar_url: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct FriendOverview {
    pub friends: Vec<FriendItem>,
    pub incoming_requests: Vec<FriendRequestItem>,
    pub outgoing_requests: Vec<FriendRequestItem>,
}

pub async fn overview(user_id: &str) -> AppResult<FriendOverview> {
    let friends = query_json_prefix::<FriendRecord>(FRIEND_PART, &friend_prefix(user_id))
        .await
        .map_err(ApiError::internal)?;
    let incoming =
        query_json_prefix::<FriendRequestRecord>(FRIEND_REQUEST_PART, &incoming_prefix(user_id))
            .await
            .map_err(ApiError::internal)?;
    let outgoing =
        query_json_prefix::<FriendRequestRecord>(FRIEND_REQUEST_PART, &outgoing_prefix(user_id))
            .await
            .map_err(ApiError::internal)?;

    let mut friends = friends.into_iter().map(friend_item).collect::<Vec<_>>();
    let mut incoming_requests = incoming
        .iter()
        .map(|request| request_item(request, true))
        .collect::<Vec<_>>();
    let mut outgoing_requests = outgoing
        .iter()
        .map(|request| request_item(request, false))
        .collect::<Vec<_>>();
    friends.sort_by_key(|item| item.name.to_lowercase());
    incoming_requests.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    outgoing_requests.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Ok(FriendOverview {
        friends,
        incoming_requests,
        outgoing_requests,
    })
}

pub async fn create_request(sender: &User, email: &str) -> AppResult<FriendRequestItem> {
    let recipient = auth::find_active_user_by_email(email)
        .await?
        .ok_or_else(|| ApiError::not_found("registered user not found"))?;
    if recipient.id == sender.id {
        return Err(ApiError::bad_request("you cannot add yourself as a friend"));
    }
    if are_friends(&sender.id, &recipient.id).await? {
        return Err(ApiError::bad_request("this user is already your friend"));
    }

    let pair = pair_idx(&sender.id, &recipient.id);
    if let Some(existing) = get_json::<FriendRequestRecord>(FRIEND_REQUEST_PAIR_PART, &pair)
        .await
        .map_err(ApiError::internal)?
    {
        return if existing.sender_id == sender.id {
            Err(ApiError::bad_request("friend request already sent"))
        } else {
            Err(ApiError::bad_request(
                "this user already sent you a friend request",
            ))
        };
    }

    let record = FriendRequestRecord {
        request_id: Uuid::new_v4().to_string(),
        sender_id: sender.id.clone(),
        sender_name: sender.name.clone(),
        sender_email: sender.email.clone(),
        sender_avatar_url: sender.avatar_url.clone(),
        recipient_id: recipient.id.clone(),
        recipient_name: recipient.name.clone(),
        recipient_email: recipient.email.clone(),
        recipient_avatar_url: recipient.avatar_url.clone(),
        created_at: Utc::now().to_rfc3339(),
    };
    let raw = serde_json::to_string(&record).map_err(ApiError::internal)?;
    transact_write_values(
        vec![
            transaction_put(
                FRIEND_REQUEST_PART,
                incoming_idx(&recipient.id, &record.request_id),
                &raw,
                false,
            ),
            transaction_put(
                FRIEND_REQUEST_PART,
                outgoing_idx(&sender.id, &record.request_id),
                &raw,
                false,
            ),
            transaction_put(FRIEND_REQUEST_PAIR_PART, pair, &raw, true),
        ],
        vec![],
    )
    .await
    .map_err(|error| {
        if error.to_string().contains("TransactionCanceled") {
            ApiError::bad_request("friend request already exists")
        } else {
            ApiError::internal(error)
        }
    })?;
    Ok(request_item(&record, false))
}

pub async fn accept_request(recipient_id: &str, request_id: &str) -> AppResult<FriendItem> {
    validate_id(request_id)?;
    let request = get_json::<FriendRequestRecord>(
        FRIEND_REQUEST_PART,
        &incoming_idx(recipient_id, request_id),
    )
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("friend request not found"))?;
    if request.recipient_id != recipient_id {
        return Err(ApiError::forbidden(
            "only the recipient can accept this request",
        ));
    }

    let since = Utc::now().to_rfc3339();
    let sender_view = FriendRecord {
        user_id: request.sender_id.clone(),
        friend_id: request.recipient_id.clone(),
        friend_name: request.recipient_name.clone(),
        friend_email: request.recipient_email.clone(),
        friend_avatar_url: request.recipient_avatar_url.clone(),
        friends_since: since.clone(),
    };
    let recipient_view = FriendRecord {
        user_id: request.recipient_id.clone(),
        friend_id: request.sender_id.clone(),
        friend_name: request.sender_name.clone(),
        friend_email: request.sender_email.clone(),
        friend_avatar_url: request.sender_avatar_url.clone(),
        friends_since: since,
    };
    transact_write_values(
        vec![
            json_transaction_put(
                FRIEND_PART,
                friend_idx(&request.sender_id, &request.recipient_id),
                &sender_view,
            )?,
            json_transaction_put(
                FRIEND_PART,
                friend_idx(&request.recipient_id, &request.sender_id),
                &recipient_view,
            )?,
        ],
        request_deletes(&request),
    )
    .await
    .map_err(ApiError::internal)?;
    Ok(friend_item(recipient_view))
}

pub async fn dismiss_request(user_id: &str, request_id: &str) -> AppResult<()> {
    validate_id(request_id)?;
    let incoming =
        get_json::<FriendRequestRecord>(FRIEND_REQUEST_PART, &incoming_idx(user_id, request_id))
            .await
            .map_err(ApiError::internal)?;
    let request = match incoming {
        Some(request) => request,
        None => {
            get_json::<FriendRequestRecord>(FRIEND_REQUEST_PART, &outgoing_idx(user_id, request_id))
                .await
                .map_err(ApiError::internal)?
                .ok_or_else(|| ApiError::not_found("friend request not found"))?
        }
    };
    transact_write_values(vec![], request_deletes(&request))
        .await
        .map_err(ApiError::internal)
}

pub async fn remove_friend(user_id: &str, friend_id: &str) -> AppResult<()> {
    validate_id(friend_id)?;
    if !are_friends(user_id, friend_id).await? {
        return Err(ApiError::not_found("friend not found"));
    }
    transact_write_values(
        vec![],
        vec![
            transaction_delete(FRIEND_PART, friend_idx(user_id, friend_id)),
            transaction_delete(FRIEND_PART, friend_idx(friend_id, user_id)),
        ],
    )
    .await
    .map_err(ApiError::internal)
}

pub async fn are_friends(user_id: &str, friend_id: &str) -> AppResult<bool> {
    get_json::<FriendRecord>(FRIEND_PART, &friend_idx(user_id, friend_id))
        .await
        .map(|item| item.is_some())
        .map_err(ApiError::internal)
}

fn friend_item(record: FriendRecord) -> FriendItem {
    FriendItem {
        user_id: record.friend_id,
        name: record.friend_name,
        email: record.friend_email,
        avatar_url: record.friend_avatar_url,
        friends_since: record.friends_since,
    }
}

fn request_item(request: &FriendRequestRecord, incoming: bool) -> FriendRequestItem {
    if incoming {
        FriendRequestItem {
            request_id: request.request_id.clone(),
            user_id: request.sender_id.clone(),
            name: request.sender_name.clone(),
            email: request.sender_email.clone(),
            avatar_url: request.sender_avatar_url.clone(),
            created_at: request.created_at.clone(),
        }
    } else {
        FriendRequestItem {
            request_id: request.request_id.clone(),
            user_id: request.recipient_id.clone(),
            name: request.recipient_name.clone(),
            email: request.recipient_email.clone(),
            avatar_url: request.recipient_avatar_url.clone(),
            created_at: request.created_at.clone(),
        }
    }
}

fn request_deletes(request: &FriendRequestRecord) -> Vec<TransactionDelete> {
    vec![
        transaction_delete(
            FRIEND_REQUEST_PART,
            incoming_idx(&request.recipient_id, &request.request_id),
        ),
        transaction_delete(
            FRIEND_REQUEST_PART,
            outgoing_idx(&request.sender_id, &request.request_id),
        ),
        transaction_delete(
            FRIEND_REQUEST_PAIR_PART,
            pair_idx(&request.sender_id, &request.recipient_id),
        ),
    ]
}

fn json_transaction_put<T: Serialize>(
    part: &str,
    idx: String,
    value: &T,
) -> AppResult<TransactionPut> {
    Ok(transaction_put(
        part,
        idx,
        &serde_json::to_string(value).map_err(ApiError::internal)?,
        false,
    ))
}

fn transaction_put(part: &str, idx: String, value: &str, if_absent: bool) -> TransactionPut {
    TransactionPut {
        part: part.to_string(),
        idx,
        value: value.to_string(),
        if_absent,
    }
}

fn transaction_delete(part: &str, idx: String) -> TransactionDelete {
    TransactionDelete {
        part: part.to_string(),
        idx,
    }
}

fn friend_idx(user_id: &str, friend_id: &str) -> String {
    format!("user:{user_id}:friend:{friend_id}")
}

fn friend_prefix(user_id: &str) -> String {
    format!("user:{user_id}:friend:")
}

fn incoming_idx(user_id: &str, request_id: &str) -> String {
    format!("recipient:{user_id}:request:{request_id}")
}

fn incoming_prefix(user_id: &str) -> String {
    format!("recipient:{user_id}:request:")
}

fn outgoing_idx(user_id: &str, request_id: &str) -> String {
    format!("sender:{user_id}:request:{request_id}")
}

fn outgoing_prefix(user_id: &str) -> String {
    format!("sender:{user_id}:request:")
}

fn pair_idx(left: &str, right: &str) -> String {
    if left <= right {
        format!("{left}:{right}")
    } else {
        format!("{right}:{left}")
    }
}

fn validate_id(value: &str) -> AppResult<()> {
    if value.trim().is_empty() || value.contains('/') || value.contains(':') {
        Err(ApiError::bad_request("invalid id"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{friend_idx, incoming_idx, outgoing_idx, pair_idx};

    #[test]
    fn canonical_pair_key_is_order_independent() {
        assert_eq!(pair_idx("b", "a"), pair_idx("a", "b"));
    }

    #[test]
    fn builds_directional_lookup_keys() {
        assert_eq!(friend_idx("a", "b"), "user:a:friend:b");
        assert_eq!(incoming_idx("b", "r"), "recipient:b:request:r");
        assert_eq!(outgoing_idx("a", "r"), "sender:a:request:r");
    }
}

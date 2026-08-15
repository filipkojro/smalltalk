use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq)]
pub enum ResponseStatus {
    Ok,
    Err,
}

#[derive(Serialize, Deserialize)]
pub struct ConectionResponse {
    pub status: ResponseStatus,
    pub client_id: u32,
}

use std::time::Duration;

use bytes::Bytes;
use walrus::client::Client;

#[tokio::main]
async fn main() {
    let mut client = Client::connect("localhost:6379").await.unwrap();

    let set = client
        .set(
            "something".to_string(),
            Bytes::from("some value"),
            Some(Duration::from_secs(3)),
        )
        .await
        .unwrap();
    let get = client.get("something".to_string()).await.unwrap().unwrap();

    println!("set: {set}, get: {get:?}");
}

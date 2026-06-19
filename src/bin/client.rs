use std::time::Duration;

use bytes::Bytes;
use walrus::client::Client;

#[tokio::main]
async fn main() {
    let mut client = Client::connect("localhost:6379", Some(32)).await.unwrap();

    let set = client
        .set(
            Bytes::from("something"),
            Bytes::from("some value"),
            Some(Duration::from_secs(3)),
        )
        .await
        .unwrap();
    let get = client.get(Bytes::from("something")).await.unwrap().unwrap();

    println!("set: {set:?}, get: {get:?}");
}

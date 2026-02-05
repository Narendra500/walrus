use walrus::client::Client;

#[tokio::main]
async fn main() {
    let mut client = Client::connect("localhost:6379").await.unwrap();

    let pong = client.ping(None).await.unwrap();
    println!("{pong:?}");
}

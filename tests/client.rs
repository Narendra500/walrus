use walrus::client::{Client, double_to_string, int_to_string};
use walrus::db::Data;

use bytes::Bytes;
use rand::{RngExt, distr::Alphanumeric, random};
use std::{collections::VecDeque, time::Duration};
use tokio::time::{Instant, sleep_until};

use std::sync::atomic::{AtomicBool, Ordering};
static SERVER_RUNNING: AtomicBool = AtomicBool::new(false);

fn ensure_server_running() {
    if !SERVER_RUNNING.load(Ordering::Acquire) {
        std::thread::spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                if let Ok(listener) = tokio::net::TcpListener::bind("127.0.0.1:6380").await {
                    walrus::server::run(listener, 6380, None, None).await;
                }
            });
        });
        std::thread::sleep(std::time::Duration::from_millis(100));
        SERVER_RUNNING.store(true, Ordering::Release);
    }
}

async fn connect_client() -> Client {
    ensure_server_running();
    Client::connect(
        SERVER_IPADDRESS.to_string(),
        READ_BUFFER_SIZE,
        WRITE_BUFFER_SIZE,
    )
    .await
    .unwrap()
}

const SERVER_IPADDRESS: &str = "127.0.0.1:6380";
const READ_BUFFER_SIZE: Option<u16> = Some(32);
const WRITE_BUFFER_SIZE: Option<u16> = Some(32);

fn random_bytes(len: usize) -> Bytes {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(u8::from)
        .collect()
}

fn random_data_array(len: usize) -> VecDeque<Data> {
    let data_type: Vec<Data> = vec![
        Data::String("".into()),
        Data::Integer(0),
        Data::Bytes("".into()),
    ];

    let mut data_vec = VecDeque::with_capacity(len);

    for _ in 0..len {
        let data_type_index = (random::<u64>() % data_type.len() as u64) as usize;
        let data_type = data_type[data_type_index].clone();
        let data = match data_type {
            Data::String(_) => Data::String(random_bytes(6)),
            Data::Integer(_) => Data::Integer(random::<i64>()),
            Data::Bytes(_) => Data::Bytes(random_bytes(6)),
            _ => unreachable!(),
        };
        data_vec.push_back(data);
    }

    data_vec
}

#[tokio::test]
async fn ping_test() {
    let mut client = connect_client().await;
    let ping_response = client.ping(None).await.unwrap();

    assert_eq!(ping_response, Bytes::from("PONG"));
}

#[tokio::test]
async fn ping_test_with_message() {
    let message = "Hello There!".as_bytes();
    let mut client = connect_client().await;
    let ping_response = client.ping(Some(Bytes::from(message))).await.unwrap();
    println!("{ping_response:?}");

    assert_eq!(ping_response, Bytes::from(message));
}

#[tokio::test]
async fn multi_ping_test() {
    let mut client = connect_client().await;

    let mut ping_response_list = vec![];
    for _ in 0..5 {
        ping_response_list.push(client.ping(None).await.unwrap());
    }

    let pong = Bytes::from("PONG");
    for response in ping_response_list.iter() {
        assert_eq!(*response, pong);
    }
}

#[tokio::test]
async fn multi_ping_test_with_message() {
    let message = "Hello There!".as_bytes();
    let mut client = connect_client().await;

    let mut ping_response_list = vec![];
    for _ in 0..5 {
        ping_response_list.push(client.ping(Some(Bytes::from(message))).await.unwrap());
    }

    let pong = Bytes::from(message);
    for response in ping_response_list.iter() {
        println!("{:?}", *response);
        assert_eq!(*response, pong);
    }
}

#[tokio::test]
async fn set_test_no_expire() {
    let mut client = connect_client().await;

    let key = random_bytes(6);
    let value = Bytes::from("value1 value2 value3 value4");

    let set_response = client.set(key, value, None).await.unwrap();

    assert_eq!("OK", set_response);
}

/// Sets a key value pair with 1000 millisecond expiration duration.
/// Attempts to fetch teh value of the same key again after the key is expired.
/// Expected response from server is a Null frame for the get command.
#[tokio::test]
async fn set_get_test_after_expire() {
    let mut client = connect_client().await;

    let key = random_bytes(6);
    let value = Bytes::from("value1 value2 value3 value4");
    let expire = Duration::from_millis(1000);

    let now = Instant::now();
    let set_response = client.set(key.clone(), value, Some(expire)).await.unwrap();

    // OK is the expected response for successful set command
    assert_eq!("OK", set_response);

    // sleep until the key is expired, with a small margin to avoid flakiness.
    sleep_until(now + expire + Duration::from_millis(100)).await;
    let get_response = client.get(key).await.unwrap();

    // the response must be None.
    match get_response {
        None => {}
        Some(response) => {
            panic!("Invalid response from server: {response:?}");
        }
    }
}

/// Sets a key value pair with 1000 millisecond expiration.
/// Attempts to fetch the value of the same key before the key expires.
/// The expected response is a Bulk frame containing the value of the key.
#[tokio::test]
async fn set_get_test_before_expire() {
    let mut client = connect_client().await;

    let key = random_bytes(6);
    let original_value = Bytes::from("value1 value2 value3 value4");
    let expire = Duration::from_millis(1000);

    let now = Instant::now();
    let set_response = client
        .set(key.clone(), original_value.clone(), Some(expire))
        .await
        .unwrap();

    // OK is the expected respones for successful set command.
    assert_eq!("OK", set_response);

    // If the key isn't expired yet attempt to fetch it.
    if Instant::now() < now + expire {
        let get_response = client.get(key).await.unwrap().unwrap();
        assert_eq!(get_response, original_value);
    } else {
        println!("The key expired before sending the get command.");
    }
}

#[tokio::test]
async fn get_double_trailing_zeros_test() {
    let mut client = connect_client().await;
    let key = random_bytes(6);
    let value = "5000.00";

    let set_response = client
        .set(key.clone(), Bytes::from(value), None)
        .await
        .unwrap();
    println!("set_response: {set_response:?}");

    assert_eq!("OK", set_response);

    let get_response = client.get(key).await.unwrap().unwrap();
    println!("get_response: {get_response:?}");

    assert_eq!(get_response, Bytes::from(value));
}

/// Pushes a list containing Data into server db.
/// Checks if the response is not zero.
#[tokio::test]
async fn rpush_test() {
    let mut client = connect_client().await;

    let list_key = random_bytes(6);
    let data = random_data_array(3);
    let len = data.len() as i64;

    let rpush_response = client.rpush(list_key, data).await.unwrap();
    println!("rpush_response: {rpush_response}");

    assert_eq!(rpush_response, len as i64);
}

/// Creates a list with key `list_key` and then pushes another list to the front of the list.
/// Checks if the length of the list is the sum of the two lists.
#[tokio::test]
async fn lpush_test() {
    let mut client = connect_client().await;

    let list_key = random_bytes(6);
    let data = random_data_array(3);
    let len = data.len() as i64;

    let rpush_response = client.rpush(list_key.clone(), data).await.unwrap();
    assert_eq!(rpush_response, len);

    let data2 = VecDeque::from([
        Data::String(random_bytes(6)),
        Data::Integer(random::<i64>()),
        Data::Bytes(Bytes::from(random_bytes(6))),
    ]);
    let len2 = data2.len() as i64;
    let lpush_response = client.lpush(list_key, data2).await.unwrap();

    assert_eq!(lpush_response, len + len2);
}

/// Pushes a list to the server db and then requests the full list back.
/// checks if the returned list has same elements as the one sent originally.
/// start is 0 and end is length of list - 1.
#[tokio::test]
async fn lrange_test_full_range() {
    let mut client = connect_client().await;

    let list_key = random_bytes(6);
    let data = random_data_array(3);
    let len = data.len() as i64;

    // Send data to create the list with.
    let rpush_response = client.rpush(list_key.clone(), data.clone()).await.unwrap();
    println!("rpush_response: {rpush_response}");

    assert_eq!(rpush_response, len);

    // Get back all elements of the list.
    let start_index = 0;
    let end_index = -1;
    let lrange_response = client
        .lrange(list_key, start_index, end_index)
        .await
        .unwrap();

    assert_eq!(data, lrange_response);
}

/// Pushes a list to the server db and then requests the full list back.
/// checks if the returned list has same elements as the one sent originally.
/// start is -(length of list * 2) this ensures that the final value of start is
/// negative and end is length of list. This ensures that the requested range is superset of
/// the actual list range.
#[tokio::test]
async fn lrange_out_of_bounds_test() {
    let mut client = connect_client().await;

    let list_key = random_bytes(6);
    let mut data = random_data_array(3);
    let len = data.len() as i64;

    // Send data to create the list with.
    let rpush_response = client.lpush(list_key.clone(), data.clone()).await.unwrap();
    println!("rpush_response: {rpush_response}");

    assert_eq!(rpush_response, len);

    // Get back all elements of the list.
    let start_index = -(len * 2);
    let end_index = len;
    let lrange_response = client
        .lrange(list_key, start_index, end_index)
        .await
        .unwrap();
    println!("lrange_response: {lrange_response:?}");

    data.make_contiguous().reverse();
    assert_eq!(data, lrange_response);
}

/// Get's back the last two elements of the list using negative indices.
#[tokio::test]
async fn lrange_test_negative_indices() {
    let mut client = connect_client().await;
    let list_key = random_bytes(8);

    let data = VecDeque::from([Data::Integer(1), Data::Integer(2), Data::Integer(3)]);

    client.rpush(list_key.clone(), data).await.unwrap();

    // Get the last two elements using negative indices [-2, -1]
    let res = client.lrange(list_key, -2, -1).await.unwrap();

    assert_eq!(res.len(), 2);
    assert_eq!(res[0], Data::Integer(2));
    assert_eq!(res[1], Data::Integer(3));
}

/// Pushes a list to the server db and then requests the length of the list.
/// checks if the returned length is same as the one sent originally.
#[tokio::test]
async fn llen_test() {
    let mut client = connect_client().await;

    let list_key = random_bytes(6);
    let data = random_data_array(3);
    let len = data.len() as i64;

    // Send data to create the list with.
    let rpush_response = client.rpush(list_key.clone(), data).await.unwrap();
    println!("rpush_response: {rpush_response}");

    assert_eq!(rpush_response, len);

    // Get back the length of the list.
    let llen_response = client.llen(list_key).await.unwrap();
    assert_eq!(llen_response, len);
}

/// Test for `LPop` command without specifying the count, returns the first element of the list with key.
#[tokio::test]
async fn lpop_no_count() {
    let mut client = connect_client().await;

    let list_key = random_bytes(6);
    let data = random_data_array(6);

    // Send data to create the list with.
    let rpush_response = client.rpush(list_key.clone(), data.clone()).await.unwrap();
    println!("rpush_response: {rpush_response}");

    assert_eq!(rpush_response, data.len() as i64);

    let lpop_response = client.lpop(list_key, None).await.unwrap().unwrap();
    println!("lpop_response: {lpop_response:?}");

    assert_eq!(lpop_response, data.range(..1).cloned().collect::<Vec<_>>());
}

/// Test for `LPop` command, returns the first element of the list with key.
#[tokio::test]
async fn lpop_test_first_only() {
    let mut client = connect_client().await;

    let list_key = random_bytes(6);
    let data = random_data_array(6);

    // Send data to create the list with.
    let rpush_response = client.rpush(list_key.clone(), data.clone()).await.unwrap();
    println!("rpush_response: {rpush_response}");

    assert_eq!(rpush_response, data.len() as i64);

    // Get back the first element of the list.
    let count = 1;
    let lpop_response = client.lpop(list_key, Some(count)).await.unwrap().unwrap();
    println!("lpop_response: {lpop_response:?}");

    assert_eq!(
        lpop_response,
        data.range(0..count as usize).cloned().collect::<Vec<_>>()
    );
}

/// Test for `LPop` command, returns the first `count` elements of the list with key.
#[tokio::test]
async fn lpop_test_multiple_within_bounds() {
    let mut client = connect_client().await;

    let list_key = random_bytes(6);
    let data = random_data_array(6);

    // Send data to create the list with.
    let rpush_response = client.rpush(list_key.clone(), data.clone()).await.unwrap();
    println!("rpush_response: {rpush_response}");

    assert_eq!(rpush_response, data.len() as i64);

    let count = data.len() as i64 - 1;
    let lpop_response = client.lpop(list_key, Some(count)).await.unwrap().unwrap();
    println!("lpop_response: {lpop_response:?}");

    assert_eq!(
        lpop_response,
        data.range(0..count as usize).cloned().collect::<Vec<_>>()
    );
}

#[tokio::test]
#[should_panic(expected = "value is out of range, must be positive")]
async fn lpop_test_negative_count() {
    let mut client = connect_client().await;

    let list_key = random_bytes(6);
    let data = random_data_array(6);

    // Send data to create the list with.
    let rpush_response = client.rpush(list_key.clone(), data.clone()).await.unwrap();
    println!("rpush_response: {rpush_response}");

    assert_eq!(rpush_response, data.len() as i64);

    let count = -1;
    // Panics with "Value out of range" error.
    let _ = client.lpop(list_key, Some(count)).await.unwrap();
}

#[tokio::test]
async fn blpop_test_immediate_return() {
    let mut client = connect_client().await;

    let list = random_bytes(6);
    let data = random_data_array(6);

    // First element of the list is expected to be popped.
    let expected_value = data.front().unwrap().clone();

    client.rpush(list.clone(), data).await.unwrap();

    let response = client.blpop(vec![list], 5.0).await.unwrap();

    assert!(response.is_some(), "Expected response to be Some");

    let result_array = response.unwrap();
    assert_eq!(result_array.len(), 2, "BLPOP should return [key, value]");

    assert_eq!(result_array[1], expected_value);
}

#[tokio::test]
async fn blpop_test_timeout() {
    let mut client = connect_client().await;

    let list = random_bytes(6);

    // Start a timer to measure how lone it takes for blpop response.
    let start_time = tokio::time::Instant::now();

    let response = client.blpop(vec![list], 2.0).await.unwrap();
    let elapsed_time = start_time.elapsed().as_secs();

    assert!(response.is_none(), "Expected response to be None");
    assert!(
        elapsed_time >= 2,
        "Expected elapsed time to be at least 2 seconds"
    );
}

#[tokio::test]
async fn blpop_test_concurrent_wakeup() {
    let mut client1 = connect_client().await;
    let mut client2 = connect_client().await;

    let list = random_bytes(6);
    let data = random_data_array(1);
    let expected_value = data.front().unwrap().clone();

    let key_for_task = list.clone();
    let data_for_task = data.clone();

    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        client2.rpush(key_for_task, data_for_task).await.unwrap();
    });

    let response = client1.blpop(vec![list], 5.0).await.unwrap();

    assert!(response.is_some(), "Expected response to be Some");
    let result_array = response.unwrap();
    assert_eq!(result_array.len(), 2, "BLPOP should return [key, value]");
    assert_eq!(result_array[1], expected_value);
}

#[tokio::test]
async fn blpop_test_key_priority() {
    let mut client = connect_client().await;

    let key_empty = random_bytes(6);
    let key_populated = random_bytes(6);

    let data = random_data_array(1);
    let expected_value = data[0].clone();

    // Push data ONLY to the second key
    client.rpush(key_populated.clone(), data).await.unwrap();

    // Ask to BLPOP from the empty key first, then the populated one
    let response = client
        .blpop(vec![key_empty.clone(), key_populated.clone()], 5.0)
        .await
        .unwrap();

    assert!(response.is_some());
    let result_array = response.unwrap();

    // The server should have skipped 'key_empty' and popped from 'key_populated'
    // verify the key name returned by the server matches `key_populated`
    let returned_key = match &result_array[0] {
        Data::String(data) | Data::Bytes(data) => data.clone(),
        _ => panic!("Expected key name to be a string or bytes"),
    };

    assert_eq!(returned_key, key_populated, "Popped from the wrong key!");
    assert_eq!(result_array[1], expected_value);
}

#[tokio::test]
async fn wtype_test_list() {
    let mut client = connect_client().await;

    let key = random_bytes(6);
    let value = random_data_array(3);

    client.rpush(key.clone(), value.clone()).await.unwrap();

    let wtype_response = client.wtype(key).await.unwrap();
    assert_eq!(wtype_response, "list");
}

#[tokio::test]
async fn wtype_test_string() {
    let mut client = connect_client().await;

    let key = random_bytes(6);
    let value = random_bytes(6);

    client.set(key.clone(), value.into(), None).await.unwrap();

    let wtype_response = client.wtype(key).await.unwrap();
    assert_eq!(wtype_response, "string");
}

#[tokio::test]
async fn wtype_test_integer() {
    let mut client = connect_client().await;

    let key = random_bytes(6);
    let value = int_to_string(random::<i64>());

    client.set(key.clone(), value.into(), None).await.unwrap();

    let wtype_response = client.wtype(key).await.unwrap();
    assert_eq!(wtype_response, "string");
}

#[tokio::test]
async fn wtype_test_double() {
    let mut client = connect_client().await;

    let key = random_bytes(6);
    let value = double_to_string(random::<f64>());

    client.set(key.clone(), value.into(), None).await.unwrap();

    let wtype_response = client.wtype(key).await.unwrap();
    assert_eq!(wtype_response, "string");
}

#[tokio::test]
async fn wtype_test_non_existent_key() {
    let mut client = connect_client().await;

    let key = random_bytes(6);

    let wtype_response = client.wtype(key).await.unwrap();
    assert_eq!(wtype_response, "none");
}

#[tokio::test]
async fn test_pipeline_processing() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut stream = TcpStream::connect(SERVER_IPADDRESS).await.unwrap();

    let payload = b"*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n";

    stream.write_all(payload).await.unwrap();

    let mut buffer = [0; 1024];
    let n = stream.read(&mut buffer).await.unwrap();
    let response = std::str::from_utf8(&buffer[..n]).unwrap();

    let expected_response = "$4\r\nPONG\r\n$2\r\nOK\r\n$4\r\nval1\r\n";
    assert_eq!(response, expected_response);
}

#[tokio::test]
async fn blpop_multiple_waiters_fifo_order() {
    let mut client1 = connect_client().await;
    let mut client2 = connect_client().await;
    let mut client3 = connect_client().await;
    let mut client4 = connect_client().await;

    let list_key = random_bytes(8);

    // Spawn client1 BLPOP
    let list_key1 = list_key.clone();
    let handle1 = tokio::spawn(async move { client1.blpop(vec![list_key1], 5.0).await.unwrap() });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Spawn client2 BLPOP
    let list_key2 = list_key.clone();
    let handle2 = tokio::spawn(async move { client2.blpop(vec![list_key2], 5.0).await.unwrap() });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Spawn client3 BLPOP
    let list_key3 = list_key.clone();
    let handle3 = tokio::spawn(async move { client3.blpop(vec![list_key3], 5.0).await.unwrap() });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Push the first element
    let mut data1 = VecDeque::new();
    data1.push_back(Data::Bytes(Bytes::from("val1")));
    client4.rpush(list_key.clone(), data1).await.unwrap();

    // The first waiter (client1) should be woken up and receive val1
    let res1 = handle1.await.unwrap();
    assert!(res1.is_some());
    assert_eq!(res1.unwrap()[1], Data::Bytes(Bytes::from("val1")));

    // Verify other waiters are still blocked (not resolved yet)
    assert!(!handle2.is_finished());
    assert!(!handle3.is_finished());

    // Push the second element
    let mut data2 = VecDeque::new();
    data2.push_back(Data::Bytes(Bytes::from("val2")));
    client4.rpush(list_key.clone(), data2).await.unwrap();

    // The second waiter (client2) should be woken up and receive val2
    let res2 = handle2.await.unwrap();
    assert!(res2.is_some());
    assert_eq!(res2.unwrap()[1], Data::Bytes(Bytes::from("val2")));

    assert!(!handle3.is_finished());
}

#[tokio::test]
async fn test_high_concurrency_set_get() {
    use futures::future::join_all;

    let mut handles = vec![];

    // Spawn 50 tasks concurrently executing SET/GET operations
    for i in 0..50 {
        handles.push(tokio::spawn(async move {
            let mut client = connect_client().await;
            let key = Bytes::from(format!("concurrent_key_{}", i));
            let val = Bytes::from(format!("val_{}", i));

            // Perform multiple SET/GET cycles to check thread safety
            for _ in 0..10 {
                client.set(key.clone(), val.clone(), None).await.unwrap();
                let res = client.get(key.clone()).await.unwrap().unwrap();
                assert_eq!(res, val);
            }
        }));
    }

    join_all(handles).await;
}

#[tokio::test]
async fn test_defensive_parsing_malformed_protocol() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    ensure_server_running();

    let mut stream = TcpStream::connect(SERVER_IPADDRESS).await.unwrap();

    // Send a completely corrupted/malformed command frame
    let malformed_payload = b"GET\r\n*999999999999999999999999999999\r\n";
    stream.write_all(malformed_payload).await.unwrap();

    let mut buffer = [0; 1024];
    // The server should reject the malformed protocol and close the connection
    let n = stream.read(&mut buffer).await.unwrap();
    assert_eq!(n, 0, "Server should close connection on malformed protocol");
}

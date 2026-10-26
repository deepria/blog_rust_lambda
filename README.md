# Introduction 

blog_rust_lambda is a Rust project that implements an AWS Lambda function in Rust.

## Prerequisites

- [Rust](https://www.rust-lang.org/tools/install)
- [Cargo Lambda](https://www.cargo-lambda.info/guide/installation.html)

## Building

To build the project for production, run `cargo lambda build --release`. Remove the `--release` flag to build for development.

Read more about building your lambda function in [the Cargo Lambda documentation](https://www.cargo-lambda.info/commands/build.html).

## Testing

You can run regular Rust unit tests with `cargo test`.

If you want to run integration tests locally, you can use the `cargo lambda watch` and `cargo lambda invoke` commands to do it.

First, run `cargo lambda watch` to start a local server. When you make changes to the code, the server will automatically restart.

Second, you'll need a way to pass the event data to the lambda function.

You can use the existent [event payloads](https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/lambda-events/src/fixtures) in the Rust Runtime repository if your lambda function is using one of the supported event types.

You can use those examples directly with the `--data-example` flag, where the value is the name of the file in the [lambda-events](https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/lambda-events/src/fixtures) repository without the `example_` prefix and the `.json` extension.

```bash
cargo lambda invoke --data-example apigw-request
```

For generic events, where you define the event data structure, you can create a JSON file with the data you want to test with. For example:

```json
{
    "command": "test"
}
```

Then, run `cargo lambda invoke --data-file ./data.json` to invoke the function with the data in `data.json`.

For HTTP events, you can also call the function directly with cURL or any other HTTP client. For example:

```bash
curl https://localhost:9000
```

Read more about running the local server in [the Cargo Lambda documentation for the `watch` command](https://www.cargo-lambda.info/commands/watch.html).
Read more about invoking the function in [the Cargo Lambda documentation for the `invoke` command](https://www.cargo-lambda.info/commands/invoke.html).

## Deploying

To deploy the project, run `cargo lambda deploy`. This will create an IAM role and a Lambda function in your AWS account.

Read more about deploying your lambda function in [the Cargo Lambda documentation](https://www.cargo-lambda.info/commands/deploy.html).

## User settings API

Both endpoints require an authenticated session and use the authenticated user's ID;
clients cannot read or update another user's settings.

- `GET /api/settings` returns `{"data":{"dashboard_gallery_enabled":true}}`
  for users with no saved settings.
- `PUT /api/settings` accepts `{"dashboard_gallery_enabled":false}` and returns
  the saved settings in the same `data` envelope. The field is required and must be
  a JSON boolean; malformed bodies, missing fields, and unknown fields return HTTP 400.
- Settings persist per account in the existing DynamoDB table under
  `part=USER_SETTINGS`, `idx=user:<user_id>`. No table migration is required.
- The frontend can fetch these settings on another device or when returning to the
  dashboard. This endpoint does not provide live push updates to already open tabs.

## File registration timestamps

`GET /api/files` and `GET /api/files/shared-with-me` include `created_at` on each
item: Unix seconds from the owner's file metadata, or `null` for legacy files
without a recorded timestamp. Registration time is the upload presign or multipart
initiation time, matching the metadata already recorded by the upload flow; it is
not upload completion time. Shared items retain their separate `shared_at` field.
Clients can sort by `created_at` without an additional sorting endpoint or database
migration.

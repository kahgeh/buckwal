//! `notify-setup`: prints what to set up on the bucket so replicas hear about new log
//! objects. It runs nothing, reads no secret and makes no network call.

/// The most replicas one run prints commands for.
const MAX_REPLICAS: u32 = 1000;

/// The two object kinds whose new objects a replica follows, as S3 key prefix filters. A
/// format 5 bucket has no `manifest/` and no `pages/` objects.
const PREFIXES: [&str; 2] = ["log/", "head"];

/// A bucket name as S3 allows it, checked loosely: 3 to 63 of lowercase letters, digits, dots
/// and hyphens, starting and ending with a letter or digit.
fn check_bucket(bucket: &str) -> Result<(), String> {
    let edge_ok = |c: Option<char>| c.is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    let ok = (3..=63).contains(&bucket.len())
        && bucket
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-')
        && edge_ok(bucket.chars().next())
        && edge_ok(bucket.chars().last());
    if ok {
        Ok(())
    } else {
        Err(format!(
            "--bucket {bucket:?} is not a bucket name: 3 to 63 lowercase letters, digits, dots or hyphens, starting and ending with a letter or digit"
        ))
    }
}

/// A topic name as SNS allows it: 1 to 256 letters, digits, hyphens and underscores.
fn check_topic(topic: &str) -> Result<(), String> {
    let ok = (1..=256).contains(&topic.len())
        && topic
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if ok {
        Ok(())
    } else {
        Err(format!(
            "--topic {topic:?} is not a topic name: 1 to 256 letters, digits, hyphens or underscores"
        ))
    }
}

fn check_region(region: &str) -> Result<(), String> {
    let ok = !region.is_empty()
        && region
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if ok {
        Ok(())
    } else {
        Err(format!(
            "--region {region:?} is not a region name (for example eu-west-1)"
        ))
    }
}

/// The notification rules for `target_key` (`TopicArn` for SNS, `QueueArn` for RustFS) as the
/// JSON `put-bucket-notification-configuration` takes: one rule per prefix, since a rule's
/// filter holds one prefix.
fn notification_json(config_key: &str, arn_key: &str, arn: &str) -> String {
    let rules: Vec<String> = PREFIXES
        .iter()
        .map(|p| {
            format!(
                "    {{\n      \"Id\": \"buckwal-{id}\",\n      \"{arn_key}\": \"{arn}\",\n      \"Events\": [\"s3:ObjectCreated:*\"],\n      \"Filter\": {{ \"Key\": {{ \"FilterRules\": [{{ \"Name\": \"prefix\", \"Value\": \"{p}\" }}] }} }}\n    }}",
                id = p.trim_end_matches('/')
            )
        })
        .collect();
    format!("{{\n  \"{config_key}\": [\n{}\n  ]\n}}", rules.join(",\n"))
}

/// The text to print for `bucket` and `replicas` replicas. Pure: no environment, no secret.
pub fn render(bucket: &str, replicas: u32, region: Option<&str>, topic: Option<&str>) -> String {
    let region = region.unwrap_or("REGION");
    let topic = topic.unwrap_or("buckwal-replicas");
    let arn = format!("arn:aws:sns:{region}:ACCOUNT_ID:{topic}");
    let mut o = String::new();
    let mut line = |t: &str| {
        o.push_str(t);
        o.push('\n');
    };
    line("# buckwal notify-setup: nothing below has been run, and nothing was sent to AWS.");
    line("# Replace ACCOUNT_ID (and REGION, REPLICA_<n>_HOST, PORT) before you run a command.");
    line("# Notifications only start a replica's next pass sooner; without them a replica");
    line("# still follows on its slow poll (BUCKWAL_REPLICA_POLL_MS).");
    line("");
    line("## 1. AWS: an SNS topic that the bucket may publish to");
    line("");
    line(&format!(
        "aws sns create-topic --region {region} --name {topic}"
    ));
    line("");
    line("# topic-policy.json");
    line(&format!(
        "{{\n  \"Version\": \"2012-10-17\",\n  \"Statement\": [\n    {{\n      \"Sid\": \"AllowS3PublishFromBucket\",\n      \"Effect\": \"Allow\",\n      \"Principal\": {{ \"Service\": \"s3.amazonaws.com\" }},\n      \"Action\": \"SNS:Publish\",\n      \"Resource\": \"{arn}\",\n      \"Condition\": {{\n        \"ArnLike\": {{ \"aws:SourceArn\": \"arn:aws:s3:::{bucket}\" }},\n        \"StringEquals\": {{ \"aws:SourceAccount\": \"ACCOUNT_ID\" }}\n      }}\n    }}\n  ]\n}}"
    ));
    line("");
    line(&format!(
        "aws sns set-topic-attributes --region {region} --topic-arn {arn} --attribute-name Policy --attribute-value file://topic-policy.json"
    ));
    line("");
    line(&format!(
        "## 2. AWS: one HTTPS subscription per replica ({replicas})"
    ));
    line("");
    line("# <token> stands for the contents of the replica's BUCKWAL_REPLICA_NOTIFY_TOKEN_FILE.");
    line("# SNS keeps it in the subscription's endpoint URL, in your AWS account.");
    line("# Each replica needs a public HTTPS address with a certificate a public CA signed.");
    line("");
    for i in 1..=replicas {
        line(&format!(
            "aws sns subscribe --region {region} --topic-arn {arn} --protocol https --notification-endpoint 'https://sns:<token>@REPLICA_{i}_HOST:PORT/notify'"
        ));
    }
    line("");
    line("# SNS confirms each subscription by calling the replica, which must be running.");
    line("");
    line("## 3. AWS: the bucket's notification configuration");
    line("");
    line("# Only the objects replicas follow: log/ and head.");
    line("# If the database lives under a prefix P (BUCKWAL_PREFIX), start each prefix with P/");
    line("# (for example P/log/).");
    line("");
    line("# notification.json");
    line(&notification_json("TopicConfigurations", "TopicArn", &arn));
    line("");
    line(&format!(
        "aws s3api put-bucket-notification-configuration --bucket {bucket} --notification-configuration file://notification.json"
    ));
    line("");
    line("## 4. RustFS equivalents (UNTESTED)");
    line("");
    line(
        "# UNTESTED: RustFS 1.0.0 could not be made to deliver an event to a replica. It refuses a",
    );
    line("# target on the machine itself or on a private network unless the container starts with");
    line(
        "# RUSTFS_OUTBOUND_ALLOW_ORIGINS naming that origin (comma-separated). Treat the commands",
    );
    line("# below as a starting point and check them against your RustFS version.");
    line("#");
    line(
        "# One webhook target per replica, sent straight to the replica's listener (no SNS, so no",
    );
    line("# basic authentication; auth_token is the token RustFS sends). Run with your admin");
    line("# credentials, once per replica, replacing <n>, REPLICA_<n>_HOST and PORT:");
    line("#   curl --aws-sigv4 aws:amz:us-east-1:s3 --user ACCESS_KEY:SECRET_KEY -X PUT \\");
    line("#     RUSTFS_ENDPOINT/rustfs/admin/v3/target/notify_webhook/replica<n> \\");
    line("#     -d '{\"key_values\":[{\"key\":\"enable\",\"value\":\"on\"},{\"key\":\"endpoint\",\"value\":\"https://REPLICA_<n>_HOST:PORT/notify\"},{\"key\":\"auth_token\",\"value\":\"<token>\"},{\"key\":\"queue_dir\",\"value\":\"/tmp\"}]}'");
    line("#");
    line("# Then the bucket's notification configuration, the same rules with each target's ARN");
    line("# (TARGET_ARN, from RustFS's target list) in place of the SNS topic.");
    line("");
    line("# notification-rustfs.json");
    line(&notification_json(
        "QueueConfigurations",
        "QueueArn",
        "TARGET_ARN",
    ));
    line("");
    line(&format!(
        "aws s3api put-bucket-notification-configuration --endpoint-url RUSTFS_ENDPOINT --bucket {bucket} --notification-configuration file://notification-rustfs.json"
    ));
    line("");
    line("## 5. Remember");
    line("");
    line("- Each replica needs a public HTTPS address and a certificate for SNS to reach it.");
    line(
        "- Events sent while a replica is down are lost, not queued; the replica catches up on its",
    );
    line("  first pass after it starts and on its slow poll.");
    line("- When you stop a replica for good, remove its subscription, or SNS keeps retrying:");
    line("  aws sns unsubscribe --region REGION --subscription-arn SUBSCRIPTION_ARN");
    line("  (aws sns list-subscriptions-by-topic lists the ARNs).");
    o
}

/// Check the settings and print [`render`]'s text. Makes no network call.
pub fn run(
    bucket: &str,
    replicas: u32,
    region: Option<&str>,
    topic: Option<&str>,
) -> Result<(), String> {
    check_bucket(bucket)?;
    if replicas == 0 || replicas > MAX_REPLICAS {
        return Err(format!("--replicas must be from 1 to {MAX_REPLICAS}"));
    }
    region.map(check_region).transpose()?;
    topic.map(check_topic).transpose()?;
    crate::say_all(
        "",
        &[render(bucket, replicas, region, topic)
            .trim_end()
            .to_string()],
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn out() -> String {
        render("my-bucket", 3, Some("eu-west-1"), None)
    }

    /// The JSON documents embedded in the output: each from a line that is exactly `{` to the
    /// next line that is exactly `}`.
    fn json_blocks(text: &str) -> Vec<String> {
        let mut blocks = Vec::new();
        let mut cur: Option<String> = None;
        for line in text.lines() {
            match (&mut cur, line) {
                (None, "{") => cur = Some("{\n".to_string()),
                (Some(b), "}") => {
                    b.push_str("}\n");
                    blocks.push(cur.take().unwrap());
                }
                (Some(b), l) => {
                    b.push_str(l);
                    b.push('\n');
                }
                _ => {}
            }
        }
        blocks
    }

    #[test]
    fn notify_setup_output_has_one_subscribe_per_replica() {
        let o = out();
        let subs: Vec<&str> = o
            .lines()
            .filter(|l| l.contains("aws sns subscribe"))
            .collect();
        assert_eq!(subs.len(), 3, "{o}");
        for i in 1..=3 {
            let want = format!("https://sns:<token>@REPLICA_{i}_HOST:PORT/notify");
            assert_eq!(o.matches(&want).count(), 1, "{want}\n{o}");
        }
        assert!(o.contains("--protocol https"));
        assert!(o.contains("aws sns create-topic"));
        assert!(o.contains("aws sns set-topic-attributes"));
        assert!(o.contains("aws s3api put-bucket-notification-configuration"));
        assert!(o.contains("eu-west-1"));
        assert!(o.contains("ACCOUNT_ID"));
    }

    #[test]
    fn notify_setup_never_prints_a_real_token() {
        let dir = std::env::temp_dir().join(format!("notify-setup-token-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("token");
        std::fs::write(&file, "s3cr3t-token-value\n").unwrap();
        std::env::set_var("BUCKWAL_REPLICA_NOTIFY_TOKEN_FILE", &file);
        std::env::set_var("BUCKWAL_REPLICA_NOTIFY_TOKEN", "s3cr3t-token-value");
        let o = out();
        std::env::remove_var("BUCKWAL_REPLICA_NOTIFY_TOKEN_FILE");
        std::env::remove_var("BUCKWAL_REPLICA_NOTIFY_TOKEN");
        let _ = std::fs::remove_dir_all(&dir);
        assert!(!o.is_empty());
        assert!(!o.contains("s3cr3t"), "{o}");
        assert!(o.contains("<token>"));
    }

    #[test]
    fn notify_setup_filters_exclude_pages() {
        let o = out();
        let blocks = json_blocks(&o);
        let cfg = blocks
            .iter()
            .find(|b| b.contains("NotificationConfiguration") || b.contains("TopicConfigurations"))
            .unwrap_or_else(|| panic!("no notification JSON in {o}"));
        let v: serde_json::Value = serde_json::from_str(cfg).unwrap();
        let rules = v["TopicConfigurations"].as_array().unwrap();
        let mut prefixes: Vec<String> = rules
            .iter()
            .map(|r| {
                assert_eq!(r["Events"], serde_json::json!(["s3:ObjectCreated:*"]));
                let f = &r["Filter"]["Key"]["FilterRules"];
                assert_eq!(f[0]["Name"], "prefix");
                f[0]["Value"].as_str().unwrap().to_string()
            })
            .collect();
        prefixes.sort();
        assert_eq!(prefixes, ["head", "log/"]);
        assert!(!cfg.contains("pages"), "{cfg}");
    }

    /// A format 5 bucket has no `manifest/` objects (spec "Contract changes"): no rule, in the
    /// SNS and the RustFS JSON, and no word of it in the text.
    #[test]
    fn notify_setup_prefixes_have_no_manifest() {
        let o = out();
        let blocks = json_blocks(&o);
        let mut rules = 0;
        for (cfg_key, arn_key) in [
            ("TopicConfigurations", "TopicArn"),
            ("QueueConfigurations", "QueueArn"),
        ] {
            let cfg = blocks
                .iter()
                .find(|b| b.contains(cfg_key))
                .unwrap_or_else(|| panic!("no {cfg_key} in {o}"));
            let v: serde_json::Value = serde_json::from_str(cfg).unwrap();
            let mut prefixes: Vec<&str> = v[cfg_key]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    assert!(r[arn_key].is_string(), "{r}");
                    r["Filter"]["Key"]["FilterRules"][0]["Value"]
                        .as_str()
                        .unwrap()
                })
                .collect();
            prefixes.sort();
            assert_eq!(prefixes, ["head", "log/"], "{cfg_key}");
            rules += prefixes.len();
        }
        assert_eq!(rules, 4);
        assert!(!o.contains("manifest"), "{o}");
        assert!(o.contains("log/ and head"), "{o}");
    }

    #[test]
    fn notify_setup_marks_rustfs_untested() {
        let o = out();
        let i = o.find("RustFS").expect("a RustFS section");
        let section = &o[i..];
        assert!(section.contains("UNTESTED"), "{o}");
        assert!(section.contains("RUSTFS_OUTBOUND_ALLOW_ORIGINS"), "{o}");
    }

    #[test]
    fn notify_setup_json_is_valid_json() {
        let blocks = json_blocks(&out());
        assert!(blocks.len() >= 2, "topic policy and notification config");
        for b in &blocks {
            serde_json::from_str::<serde_json::Value>(b).unwrap_or_else(|e| panic!("{e}: {b}"));
        }
    }

    #[test]
    fn notify_setup_reminders_and_validation() {
        let o = out();
        assert!(o.contains("public HTTPS"));
        assert!(o.contains("lost"));
        assert!(o.contains("aws sns unsubscribe"));
        assert!(render("b", 1, None, None).contains("REGION"));
        for bad in ["", "ab", "Has_Upper", "-lead", "trail-", "a b c", "x/y"] {
            assert!(run(bad, 1, None, None).is_err(), "{bad:?}");
        }
        assert!(run("my-bucket", 0, None, None).is_err());
    }
}

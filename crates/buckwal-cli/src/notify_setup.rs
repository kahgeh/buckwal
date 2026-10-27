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

/// The start of every RustFS command: the keys reach curl on its standard input, not in its
/// arguments, and `x-amz-content-sha256` is set as the signed calls of stage 1 set it.
const CURL: &str = "printf 'user = \"%s:%s\"\\n' \"$RUSTFS_ACCESS_KEY\" \"$RUSTFS_SECRET_KEY\" | curl -K - --aws-sigv4 aws:amz:us-east-1:s3 -H 'x-amz-content-sha256: UNSIGNED-PAYLOAD'";

/// The bucket notification as the XML RustFS takes: one `QueueConfiguration` per prefix, each
/// naming the target `replica<n>`.
fn notification_xml() -> String {
    let rules: Vec<String> = PREFIXES
        .iter()
        .map(|p| {
            format!(
                "  <QueueConfiguration><Id>buckwal-{id}</Id><Queue>arn:rustfs:sqs:us-east-1:replica<n>:webhook</Queue><Event>s3:ObjectCreated:*</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>{p}</Value></FilterRule></S3Key></Filter></QueueConfiguration>",
                id = p.trim_end_matches('/')
            )
        })
        .collect();
    format!(
        "<NotificationConfiguration>\n{}\n</NotificationConfiguration>",
        rules.join("\n")
    )
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
    line("## 4. RustFS equivalents");
    line("");
    line("# RustFS 1.0.0 takes these calls as curl --aws-sigv4 commands (curl 7.75 or later).");
    line("# The AWS CLI cannot send them. One webhook target per replica, sent straight to the");
    line(
        "# replica's listener (no SNS, so no basic authentication; auth_token is the token RustFS",
    );
    line(
        "# sends). Replace RUSTFS_ENDPOINT, REPLICA_<n>_HOST and PORT. If RUSTFS_ENDPOINT is HTTPS",
    );
    line("# with your own CA, add --cacert CA_FILE to each curl command.");
    line("#");
    line("# Set your RustFS admin keys first, with no key on the command line, so that no key stays in");
    line("# your shell history or shows in the process list:");
    line("#   read -r RUSTFS_ACCESS_KEY; read -rs RUSTFS_SECRET_KEY; export RUSTFS_ACCESS_KEY RUSTFS_SECRET_KEY");
    line("#");
    line("# Each command gives the keys to curl on its standard input. printf is a shell builtin, so");
    line("# the keys do not show in the process list.");
    line("");
    line("# Step 1. Start the container with RUSTFS_OUTBOUND_ALLOW_ORIGINS naming each listener's");
    line("# origin (comma-separated, for example https://REPLICA_1_HOST:PORT). RustFS refuses a target");
    line("# on the machine itself or on a private network without it. For an HTTPS listener, mount the");
    line("# CA that signed its certificate read only and set SSL_CERT_FILE to that file.");
    line("");
    line("# Step 2. Turn on notifications:");
    line(&format!("{CURL} \\"));
    line("  -X PUT RUSTFS_ENDPOINT/rustfs/admin/v3/module-switches \\");
    line("  -d '{\"notify_enabled\":true,\"audit_enabled\":false}'");
    line("");
    line("# Step 3. Start the replica (buckwal follow), so that its listener runs. RustFS checks the");
    line("# target's endpoint when it creates the target.");
    line("");
    line("# Step 4. Create one webhook target per replica. <token> stands for the contents of the");
    line("# replica's BUCKWAL_REPLICA_NOTIFY_TOKEN_FILE. The token goes into a file that only you can");
    line("# read, not onto the command line.");
    for i in 1..=replicas {
        line("");
        line(&format!("install -m 600 /dev/null target-replica{i}.json"));
        line(&format!("# target-replica{i}.json"));
        line(&format!(
            "{{\n  \"key_values\": [\n    {{ \"key\": \"enable\", \"value\": \"on\" }},\n    {{ \"key\": \"endpoint\", \"value\": \"https://REPLICA_{i}_HOST:PORT/notify\" }},\n    {{ \"key\": \"auth_token\", \"value\": \"<token>\" }},\n    {{ \"key\": \"queue_dir\", \"value\": \"/tmp\" }}\n  ]\n}}"
        ));
        line(&format!("{CURL} \\"));
        line(&format!(
            "  -X PUT RUSTFS_ENDPOINT/rustfs/admin/v3/target/notify_webhook/replica{i} \\"
        ));
        line(&format!("  --data-binary @target-replica{i}.json"));
        line(&format!(
            "# The token stays in target-replica{i}.json until you remove it:"
        ));
        line(&format!("rm target-replica{i}.json"));
    }
    line("");
    line("# Step 5. Set the bucket's notification configuration. RustFS takes it as XML. Replace <n>");
    line(
        "# with the number of the target (replica<n>). For more than one replica, add the same two",
    );
    line("# QueueConfiguration elements for each target.");
    line("# If the database lives under a prefix P (BUCKWAL_PREFIX), start each prefix with P/.");
    line("");
    line("# notification-rustfs.xml");
    line(&notification_xml());
    line("");
    line(&format!("{CURL} \\"));
    line(&format!(
        "  -X PUT \"RUSTFS_ENDPOINT/{bucket}?notification\" \\"
    ));
    line("  --data-binary @notification-rustfs.xml");
    line("");
    line("## 5. Remember");
    line("");
    line("- Each replica needs a public HTTPS address and a certificate for SNS to reach it.");
    line("- With SNS, events sent while a replica is down are lost, not queued. With RustFS, the");
    line("  target's queue_dir keeps them. In both cases the replica catches up on its first pass");
    line("  after it starts and on its slow poll.");
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
        let aws = &o[..o.find("## 4.").unwrap()];
        assert!(aws.contains("aws s3api put-bucket-notification-configuration"));
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

    /// The text of section 4 (RustFS), up to section 5.
    fn rustfs_section(o: &str) -> &str {
        let i = o.find("## 4.").expect("section 4");
        let j = o.find("## 5.").expect("section 5");
        &o[i..j]
    }

    /// The RustFS curl commands in section 4, each joined from its continuation lines.
    fn rustfs_commands(o: &str) -> Vec<String> {
        let mut cmds = Vec::new();
        let mut cur: Option<String> = None;
        for l in rustfs_section(o).lines() {
            match &mut cur {
                None if l.starts_with("printf ") => cur = Some(l.to_string()),
                Some(c) => {
                    c.push('\n');
                    c.push_str(l);
                }
                None => {}
            }
            if cur.as_ref().is_some_and(|c| !c.ends_with('\\')) {
                cmds.push(cur.take().unwrap());
            }
        }
        cmds
    }

    /// A format 5 bucket has no `manifest/` objects (spec "Contract changes"): no rule, in the
    /// SNS JSON and the RustFS XML, and no word of it in the text.
    #[test]
    fn notify_setup_prefixes_have_no_manifest() {
        let o = out();
        let blocks = json_blocks(&o);
        let cfg = blocks
            .iter()
            .find(|b| b.contains("TopicConfigurations"))
            .unwrap_or_else(|| panic!("no TopicConfigurations in {o}"));
        let v: serde_json::Value = serde_json::from_str(cfg).unwrap();
        let mut prefixes: Vec<&str> = v["TopicConfigurations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                r["Filter"]["Key"]["FilterRules"][0]["Value"]
                    .as_str()
                    .unwrap()
            })
            .collect();
        prefixes.sort();
        assert_eq!(prefixes, ["head", "log/"]);
        let section = rustfs_section(&o);
        assert_eq!(section.matches("<QueueConfiguration>").count(), 2, "{o}");
        for p in PREFIXES {
            let want = format!("<Value>{p}</Value>");
            assert_eq!(section.matches(&want).count(), 1, "{want}\n{o}");
        }
        assert!(!o.contains("manifest"), "{o}");
        assert!(o.contains("log/ and head"), "{o}");
    }

    #[test]
    fn notify_setup_rustfs_xml_has_one_rule_per_prefix() {
        let o = out();
        let section = rustfs_section(&o);
        assert!(section.contains("notification-rustfs.xml"), "{o}");
        assert_eq!(
            section
                .matches("<Queue>arn:rustfs:sqs:us-east-1:replica<n>:webhook</Queue>")
                .count(),
            2,
            "{o}"
        );
        assert_eq!(
            section.matches("<Event>s3:ObjectCreated:*</Event>").count(),
            2
        );
        assert_eq!(
            section
                .matches("<Filter><S3Key><FilterRule><Name>prefix</Name>")
                .count(),
            2
        );
        assert!(!o.contains("QueueConfigurations"), "{o}");
        assert!(!section.contains("aws s3api"), "{o}");
        assert!(!section.contains("--endpoint-url"), "{o}");
        assert!(!section.contains("notification-rustfs.json"), "{o}");
    }

    #[test]
    fn notify_setup_rustfs_commands_are_curl_with_keys_on_stdin() {
        let o = out();
        let section = rustfs_section(&o);
        assert!(section.contains("curl 7.75"), "{o}");
        assert!(
            section.contains("read -r RUSTFS_ACCESS_KEY; read -rs RUSTFS_SECRET_KEY; export RUSTFS_ACCESS_KEY RUSTFS_SECRET_KEY"),
            "{o}"
        );
        assert!(section.contains("shell history"), "{o}");
        assert!(section.contains("process list"), "{o}");
        let cmds = rustfs_commands(&o);
        // module-switches, 3 targets, the bucket notification
        assert_eq!(cmds.len(), 5, "{cmds:?}\n{o}");
        for c in &cmds {
            assert!(
                c.starts_with("printf 'user = \"%s:%s\"\\n' \"$RUSTFS_ACCESS_KEY\" \"$RUSTFS_SECRET_KEY\" | curl -K - --aws-sigv4 aws:amz:us-east-1:s3"),
                "{c}"
            );
        }
        assert!(!section.contains("--user"), "{o}");
        for key in ["ACCESS_KEY", "SECRET_KEY"] {
            for (i, _) in section.match_indices(key) {
                assert!(section[..i].ends_with("RUSTFS_"), "{key}\n{o}");
            }
        }
        assert!(!section.contains("ACCESS_KEY:"), "{o}");
        assert!(cmds[0].contains("module-switches"));
        assert!(cmds[0].contains("{\"notify_enabled\":true,\"audit_enabled\":false}"));
        assert!(cmds[4].contains("?notification"));
        assert!(cmds[4].contains("@notification-rustfs.xml"));
    }

    #[test]
    fn notify_setup_rustfs_steps_run_in_the_working_order() {
        let o = out();
        let section = rustfs_section(&o);
        let at = |needle: &str| {
            section
                .find(needle)
                .unwrap_or_else(|| panic!("no {needle:?} in {o}"))
        };
        assert!(at("RUSTFS_OUTBOUND_ALLOW_ORIGINS") < at("module-switches"));
        assert!(at("SSL_CERT_FILE") < at("module-switches"));
        assert!(at("module-switches") < at("Start the replica"));
        assert!(at("Start the replica") < at("notify_webhook/replica1"));
        assert!(at("notify_webhook/replica3") < at("?notification"));
        for n in 1..=3 {
            let file = format!("target-replica{n}.json");
            let install = at(&format!("install -m 600 /dev/null {file}"));
            let body = section[install..].find("\"auth_token\"").unwrap() + install;
            let put = at(&format!("notify_webhook/replica{n}"));
            let rm = at(&format!("rm {file}"));
            assert!(install < body && body < put && put < rm, "{file}\n{o}");
            assert_eq!(section.matches(&format!("rm {file}")).count(), 1);
            assert!(section.contains(&format!("--data-binary @{file}")));
        }
        assert!(section.contains("<token>"));
    }

    #[test]
    fn notify_setup_rustfs_target_files_are_json() {
        let blocks = json_blocks(&out());
        let targets: Vec<&String> = blocks.iter().filter(|b| b.contains("auth_token")).collect();
        assert_eq!(targets.len(), 3);
        for t in targets {
            let v: serde_json::Value = serde_json::from_str(t).unwrap();
            assert_eq!(v["key_values"][2]["value"], "<token>");
        }
    }

    #[test]
    fn notify_setup_rustfs_section_is_not_marked_untested() {
        let o = out();
        assert!(!o.contains("UNTESTED"), "{o}");
        assert!(!o.contains("tested in stage 1 only"), "{o}");
        assert!(
            rustfs_section(&o).contains("RUSTFS_OUTBOUND_ALLOW_ORIGINS"),
            "{o}"
        );
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
        assert!(o.contains("queue_dir"));
        assert!(o.contains("aws sns unsubscribe"));
        assert!(render("b", 1, None, None).contains("REGION"));
        for bad in ["", "ab", "Has_Upper", "-lead", "trail-", "a b c", "x/y"] {
            assert!(run(bad, 1, None, None).is_err(), "{bad:?}");
        }
        assert!(run("my-bucket", 0, None, None).is_err());
    }
}

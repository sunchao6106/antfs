use crate::error::S3Error;
use crate::metastore::types::*;

pub fn serialize_list_all_my_buckets(buckets: &[BucketMeta]) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Buckets>");
    for b in buckets {
        xml.push_str(&format!(
            "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
            quick_xml::escape::escape(&b.name),
            epoch_to_iso(b.created_at),
        ));
    }
    xml.push_str("</Buckets></ListAllMyBucketsResult>");
    xml
}

/// GetBucketAcl 的 AccessControlPolicy 视图(owner FULL_CONTROL 恒在;public-read 时
/// 追加 AllUsers READ grant,对齐真 S3 canned ACL 的可视形态)。
pub fn serialize_access_control_policy(owner: &str, acl: BucketAcl) -> String {
    let o = quick_xml::escape::escape(owner);
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Owner><ID>{o}</ID><DisplayName>{o}</DisplayName></Owner>\
         <AccessControlList>\
         <Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\
         <ID>{o}</ID><DisplayName>{o}</DisplayName></Grantee>\
         <Permission>FULL_CONTROL</Permission></Grant>"
    );
    if matches!(acl, BucketAcl::PublicRead) {
        xml.push_str(
            "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\">\
             <URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee>\
             <Permission>READ</Permission></Grant>",
        );
    }
    xml.push_str("</AccessControlList></AccessControlPolicy>");
    xml
}

pub fn serialize_list_bucket_result(
    bucket: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    max_keys: i32,
    resp: &ListObjectsResp,
) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Name>{}</Name><MaxKeys>{}</MaxKeys><KeyCount>{}</KeyCount><IsTruncated>{}</IsTruncated>",
        quick_xml::escape::escape(bucket),
        max_keys,
        resp.key_count,
        resp.is_truncated,
    );
    if let Some(p) = prefix {
        xml.push_str(&format!(
            "<Prefix>{}</Prefix>",
            quick_xml::escape::escape(p)
        ));
    }
    if let Some(d) = delimiter {
        xml.push_str(&format!(
            "<Delimiter>{}</Delimiter>",
            quick_xml::escape::escape(d)
        ));
    }
    if let Some(t) = &resp.next_continuation_token {
        xml.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            quick_xml::escape::escape(t)
        ));
    }
    for e in &resp.entries {
        xml.push_str(&format!(
            "<Contents><Key>{}</Key><Size>{}</Size><ETag>{}</ETag><LastModified>{}</LastModified></Contents>",
            quick_xml::escape::escape(&e.key),
            e.size,
            quick_xml::escape::escape(&e.etag),
            epoch_to_iso(e.last_modified),
        ));
    }
    for cp in &resp.common_prefixes {
        xml.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            quick_xml::escape::escape(cp),
        ));
    }
    xml.push_str("</ListBucketResult>");
    xml
}

/// ListObjectsV1 形态(区别于 V2:恒回 Prefix/Marker(可空);无 KeyCount/
/// NextContinuationToken;NextMarker 仅 delimiter 存在且截断时回,值直接用 metastore
/// 的排他 token = 末个已扫 raw key——被 delimiter 分组吞掉的 key 不呈现也不回看;
/// 若改用「末可见项」会对组内 raw key 产生翻页重复。Contents 带 StorageClass)。
pub fn serialize_list_bucket_result_v1(
    bucket: &str,
    prefix: Option<&str>,
    marker: Option<&str>,
    delimiter: Option<&str>,
    max_keys: i32,
    resp: &ListObjectsResp,
) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Name>{}</Name><Prefix>{}</Prefix><Marker>{}</Marker><MaxKeys>{}</MaxKeys><IsTruncated>{}</IsTruncated>",
        quick_xml::escape::escape(bucket),
        quick_xml::escape::escape(prefix.unwrap_or("")),
        quick_xml::escape::escape(marker.unwrap_or("")),
        max_keys,
        resp.is_truncated,
    );
    if let Some(d) = delimiter {
        xml.push_str(&format!(
            "<Delimiter>{}</Delimiter>",
            quick_xml::escape::escape(d)
        ));
    }
    if resp.is_truncated && delimiter.is_some() {
        if let Some(next) = &resp.next_continuation_token {
            xml.push_str(&format!(
                "<NextMarker>{}</NextMarker>",
                quick_xml::escape::escape(next)
            ));
        }
    }
    for e in &resp.entries {
        xml.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size>\
             <StorageClass>STANDARD</StorageClass></Contents>",
            quick_xml::escape::escape(&e.key),
            epoch_to_iso(e.last_modified),
            quick_xml::escape::escape(&e.etag),
            e.size,
        ));
    }
    for cp in &resp.common_prefixes {
        xml.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            quick_xml::escape::escape(cp),
        ));
    }
    xml.push_str("</ListBucketResult>");
    xml
}

/// GetBucketLocation(region == us-east-1 按官方经典区行为回**空元素**内容)。
pub fn serialize_location_constraint(region: &str) -> String {
    let body = if region == "us-east-1" { "" } else { region };
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</LocationConstraint>",
        quick_xml::escape::escape(body)
    )
}

pub fn serialize_initiate_multipart(bucket: &str, key: &str, upload_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId>\
         </InitiateMultipartUploadResult>",
        quick_xml::escape::escape(bucket),
        quick_xml::escape::escape(key),
        quick_xml::escape::escape(upload_id),
    )
}

pub fn serialize_complete_multipart(bucket: &str, key: &str, etag: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Bucket>{}</Bucket><Key>{}</Key><ETag>\"{}\"</ETag>\
         </CompleteMultipartUploadResult>",
        quick_xml::escape::escape(bucket),
        quick_xml::escape::escape(key),
        quick_xml::escape::escape(etag),
    )
}

/// `PutObject + x-amz-copy-source` 的 200 应答体(CopyObjectResult;体式 ETag 带引号)。
pub fn serialize_copy_object_result(etag: &str, last_modified_ms: i64) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <LastModified>{}</LastModified><ETag>&quot;{}&quot;</ETag>\
         </CopyObjectResult>",
        epoch_to_iso(last_modified_ms),
        quick_xml::escape::escape(etag),
    )
}

/// `GET ?attributes` 的应答选集(仅请求∩支持才出项;ETag 体式裸值)。
pub struct ObjectAttributesOut {
    pub etag: Option<String>,
    pub object_size: Option<u64>,
    pub storage_class: bool,
}

pub fn serialize_object_attributes(out: &ObjectAttributesOut) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <GetObjectAttributesOutput xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    if let Some(etag) = &out.etag {
        xml.push_str(&format!("<ETag>{}</ETag>", quick_xml::escape::escape(etag)));
    }
    if let Some(size) = out.object_size {
        xml.push_str(&format!("<ObjectSize>{size}</ObjectSize>"));
    }
    if out.storage_class {
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
    }
    xml.push_str("</GetObjectAttributesOutput>");
    xml
}

pub fn serialize_list_parts(upload_id: &str, resp: &ListPartsResp) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <UploadId>{}</UploadId><IsTruncated>{}</IsTruncated>\
         <NextPartNumberMarker>{}</NextPartNumberMarker>",
        quick_xml::escape::escape(upload_id),
        resp.is_truncated,
        resp.next_part_number_marker,
    );
    for p in &resp.parts {
        xml.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><Size>{}</Size><ETag>\"{}\"</ETag></Part>",
            p.part_number,
            p.size,
            quick_xml::escape::escape(&p.etag),
        ));
    }
    xml.push_str("</ListPartsResult>");
    xml
}

pub fn serialize_list_multipart_uploads(bucket: &str, resp: &ListUploadsResp) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Bucket>{}</Bucket><IsTruncated>{}</IsTruncated>",
        quick_xml::escape::escape(bucket),
        resp.is_truncated,
    );
    if let Some(k) = &resp.next_key_marker {
        xml.push_str(&format!(
            "<NextKeyMarker>{}</NextKeyMarker>",
            quick_xml::escape::escape(k)
        ));
    }
    if let Some(u) = &resp.next_upload_id_marker {
        xml.push_str(&format!(
            "<NextUploadIdMarker>{}</NextUploadIdMarker>",
            quick_xml::escape::escape(u)
        ));
    }
    for u in &resp.uploads {
        xml.push_str(&format!(
            "<Upload><Key>{}</Key><UploadId>{}</UploadId><Initiated>{}</Initiated></Upload>",
            quick_xml::escape::escape(&u.key),
            quick_xml::escape::escape(&u.upload_id),
            epoch_to_iso(u.initiated),
        ));
    }
    xml.push_str("</ListMultipartUploadsResult>");
    xml
}

pub fn serialize_delete_result(results: &[DeleteResult], quiet: bool) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    for r in results {
        if let Some(err) = &r.error {
            xml.push_str(&format!(
                "<Error><Key>{}</Key><Code>InternalError</Code><Message>{}</Message></Error>",
                quick_xml::escape::escape(&r.key),
                quick_xml::escape::escape(err),
            ));
        } else if !quiet {
            xml.push_str(&format!(
                "<Deleted><Key>{}</Key></Deleted>",
                quick_xml::escape::escape(&r.key),
            ));
        }
    }
    xml.push_str("</DeleteResult>");
    xml
}

pub fn parse_delete_objects(xml_str: &str) -> Result<(Vec<String>, bool), S3Error> {
    #[derive(serde::Deserialize)]
    struct Delete {
        #[serde(rename = "Quiet", default)]
        quiet: Option<bool>,
        #[serde(rename = "Object", default)]
        objects: Vec<DeleteObject>,
    }
    #[derive(serde::Deserialize)]
    struct DeleteObject {
        #[serde(rename = "Key")]
        key: String,
    }
    let parsed: Delete =
        quick_xml::de::from_str(xml_str).map_err(|e| S3Error::MalformedXML(e.to_string()))?;
    let keys = parsed.objects.into_iter().map(|o| o.key).collect();
    Ok((keys, parsed.quiet.unwrap_or(false)))
}

pub fn parse_complete_multipart(xml_str: &str) -> Result<Vec<CompletePart>, S3Error> {
    #[derive(serde::Deserialize)]
    struct CompleteMultipartUpload {
        #[serde(rename = "Part")]
        parts: Vec<XmlPart>,
    }
    #[derive(serde::Deserialize)]
    struct XmlPart {
        #[serde(rename = "PartNumber")]
        part_number: i32,
        #[serde(rename = "ETag")]
        etag: String,
    }
    let parsed: CompleteMultipartUpload =
        quick_xml::de::from_str(xml_str).map_err(|e| S3Error::MalformedXML(e.to_string()))?;
    Ok(parsed
        .parts
        .into_iter()
        .map(|p| CompletePart {
            part_number: p.part_number,
            etag: p.etag,
        })
        .collect())
}

fn epoch_to_iso(epoch_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(epoch_ms)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_delete_objects_basic() {
        let xml = r#"<?xml version="1.0"?>
<Delete>
  <Object><Key>photos/2024/a.jpg</Key></Object>
  <Object><Key>photos/2024/b.jpg</Key></Object>
</Delete>"#;
        let (keys, quiet) = parse_delete_objects(xml).unwrap();
        assert_eq!(keys, vec!["photos/2024/a.jpg", "photos/2024/b.jpg"]);
        assert!(!quiet);
    }

    #[test]
    fn parse_delete_objects_quiet_mode() {
        let xml = r#"<Delete><Quiet>true</Quiet><Object><Key>k</Key></Object></Delete>"#;
        let (keys, quiet) = parse_delete_objects(xml).unwrap();
        assert_eq!(keys, vec!["k"]);
        assert!(quiet);
    }

    #[test]
    fn parse_delete_objects_empty_list() {
        let xml = "<Delete></Delete>";
        let (keys, quiet) = parse_delete_objects(xml).unwrap();
        assert!(keys.is_empty());
        assert!(!quiet);
    }

    #[test]
    fn parse_delete_objects_special_chars() {
        let xml = r#"<Delete><Object><Key>a &amp; b &lt;c&gt;</Key></Object></Delete>"#;
        let (keys, _) = parse_delete_objects(xml).unwrap();
        assert_eq!(keys, vec!["a & b <c>"]);
    }

    #[test]
    fn parse_delete_objects_malformed_xml() {
        let xml = "<Delete><Object><Key>unclosed";
        assert!(parse_delete_objects(xml).is_err());
    }

    #[test]
    fn parse_complete_multipart_basic() {
        let xml = r#"<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>"abc123"</ETag></Part>
  <Part><PartNumber>2</PartNumber><ETag>"def456"</ETag></Part>
  <Part><PartNumber>3</PartNumber><ETag>"ghi789"</ETag></Part>
</CompleteMultipartUpload>"#;
        let parts = parse_complete_multipart(xml).unwrap();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag, "\"abc123\"");
        assert_eq!(parts[2].part_number, 3);
        assert_eq!(parts[2].etag, "\"ghi789\"");
    }

    #[test]
    fn parse_complete_multipart_malformed() {
        let xml = "<CompleteMultipartUpload><Part><PartNumber>notanum</PartNumber></Part></CompleteMultipartUpload>";
        assert!(parse_complete_multipart(xml).is_err());
    }

    #[test]
    fn serialize_list_buckets_empty() {
        let xml = serialize_list_all_my_buckets(&[]);
        assert!(xml.contains("<Buckets></Buckets>"));
        assert!(xml.starts_with("<?xml"));
    }

    #[test]
    fn serialize_list_buckets_multiple() {
        let buckets = vec![
            BucketMeta {
                name: "alpha".into(),
                owner: "o".into(),
                created_at: 1700000000000,
                acl: BucketAcl::Private,
            },
            BucketMeta {
                name: "beta".into(),
                owner: "o".into(),
                created_at: 1700000001000,
                acl: BucketAcl::Private,
            },
        ];
        let xml = serialize_list_all_my_buckets(&buckets);
        assert!(xml.contains("<Name>alpha</Name>"));
        assert!(xml.contains("<Name>beta</Name>"));
        assert!(xml.contains("<CreationDate>"));
    }

    #[test]
    fn serialize_access_control_policy_all_users_grant_iff_public() {
        let private = serialize_access_control_policy("alice", BucketAcl::Private);
        assert!(private.contains("<Permission>FULL_CONTROL</Permission>"));
        assert!(!private.contains("AllUsers"), "private: {private}");
        assert_valid_xml(&private);

        let public = serialize_access_control_policy("alice", BucketAcl::PublicRead);
        assert!(public.contains("AllUsers"));
        assert!(public.contains("<Permission>READ</Permission>"));
        assert_valid_xml(&public);
    }

    #[test]
    fn serialize_list_buckets_special_chars() {
        let buckets = vec![BucketMeta {
            name: "a&b<c>".into(),
            owner: "o".into(),
            created_at: 0,
            acl: BucketAcl::Private,
        }];
        let xml = serialize_list_all_my_buckets(&buckets);
        assert!(
            xml.contains("a&amp;b&lt;c&gt;"),
            "special chars must be escaped: {xml}"
        );
    }

    #[test]
    fn serialize_list_bucket_result_empty() {
        let resp = ListObjectsResp {
            entries: vec![],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            key_count: 0,
        };
        let xml = serialize_list_bucket_result("mybucket", None, None, 1000, &resp);
        assert!(xml.contains("<Name>mybucket</Name>"));
        assert!(xml.contains("<KeyCount>0</KeyCount>"));
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml.contains("<Contents>"));
        assert!(!xml.contains("<CommonPrefixes>"));
    }

    #[test]
    fn serialize_list_bucket_result_with_entries_and_prefixes() {
        let resp = ListObjectsResp {
            entries: vec![ObjectEntry {
                key: "logs/a.log".into(),
                size: 1024,
                etag: "abc".into(),
                last_modified: 1700000000000,
            }],
            common_prefixes: vec!["logs/2024/".into(), "logs/2025/".into()],
            is_truncated: true,
            next_continuation_token: Some("logs/a.log".into()),
            key_count: 3,
        };
        let xml = serialize_list_bucket_result("b", Some("logs/"), Some("/"), 10, &resp);
        assert!(xml.contains("<Prefix>logs/</Prefix>"));
        assert!(xml.contains("<Delimiter>/</Delimiter>"));
        assert!(xml.contains("<Key>logs/a.log</Key>"));
        assert!(xml.contains("<Size>1024</Size>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<NextContinuationToken>logs/a.log</NextContinuationToken>"));
        assert!(xml.contains("<CommonPrefixes><Prefix>logs/2024/</Prefix></CommonPrefixes>"));
        assert!(xml.contains("<CommonPrefixes><Prefix>logs/2025/</Prefix></CommonPrefixes>"));
    }

    #[test]
    fn serialize_list_bucket_result_many_common_prefixes() {
        let common_prefixes: Vec<String> = (0..100).map(|i| format!("prefix-{i}/")).collect();
        let resp = ListObjectsResp {
            entries: vec![],
            common_prefixes: common_prefixes.clone(),
            is_truncated: false,
            next_continuation_token: None,
            key_count: 100,
        };
        let xml = serialize_list_bucket_result("b", None, Some("/"), 1000, &resp);
        for cp in &common_prefixes {
            assert!(xml.contains(&format!("<Prefix>{cp}</Prefix>")));
        }
    }

    #[test]
    fn serialize_initiate_multipart_basic() {
        let xml = serialize_initiate_multipart("mybucket", "bigfile.bin", "upload-123");
        assert!(xml.contains("<Bucket>mybucket</Bucket>"));
        assert!(xml.contains("<Key>bigfile.bin</Key>"));
        assert!(xml.contains("<UploadId>upload-123</UploadId>"));
    }

    #[test]
    fn v1_shape_without_delimiter() {
        let resp = ListObjectsResp {
            entries: vec![ObjectEntry {
                key: "a.txt".into(),
                size: 5,
                etag: "e1".into(),
                last_modified: 1700000000000,
            }],
            common_prefixes: vec![],
            is_truncated: true,
            next_continuation_token: Some("a.txt".into()),
            key_count: 1,
        };
        let xml = serialize_list_bucket_result_v1("b", None, None, None, 1000, &resp);
        assert!(xml.contains("<Name>b</Name>"));
        assert!(xml.contains("<Prefix></Prefix>"));
        assert!(xml.contains("<Marker></Marker>"));
        assert!(xml.contains("<MaxKeys>1000</MaxKeys>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<StorageClass>STANDARD</StorageClass>"));
        assert!(!xml.contains("<KeyCount>"));
        assert!(!xml.contains("<NextContinuationToken>"));
        assert!(!xml.contains("<NextMarker>"));
        assert!(!xml.contains("<Delimiter>"));
    }

    #[test]
    fn v1_delimiter_truncation_emits_next_marker() {
        let resp = ListObjectsResp {
            entries: vec![ObjectEntry {
                key: "zzz.txt".into(),
                size: 1,
                etag: "e".into(),
                last_modified: 0,
            }],
            common_prefixes: vec!["logs/2024/".into()],
            is_truncated: true,
            next_continuation_token: Some("zzz.txt".into()),
            key_count: 2,
        };
        let xml = serialize_list_bucket_result_v1("b", None, None, Some("/"), 2, &resp);
        assert!(xml.contains("<Delimiter>/</Delimiter>"));
        assert!(xml.contains("<NextMarker>zzz.txt</NextMarker>"));
        assert!(xml.contains("<Marker></Marker>"));
    }

    #[test]
    fn v1_next_marker_uses_exclusive_token() {
        let resp = ListObjectsResp {
            entries: vec![ObjectEntry {
                key: "a.txt".into(),
                size: 1,
                etag: "e".into(),
                last_modified: 0,
            }],
            common_prefixes: vec!["logs/".into()],
            is_truncated: true,
            next_continuation_token: Some("logs/a".into()),
            key_count: 2,
        };
        let xml = serialize_list_bucket_result_v1("b", None, None, Some("/"), 2, &resp);
        assert!(xml.contains("<NextMarker>logs/a</NextMarker>"));
    }

    #[test]
    fn v1_truncation_without_delimiter_has_no_next_marker() {
        let resp = ListObjectsResp {
            entries: vec![ObjectEntry {
                key: "a.txt".into(),
                size: 1,
                etag: "e".into(),
                last_modified: 0,
            }],
            common_prefixes: vec![],
            is_truncated: true,
            next_continuation_token: Some("a.txt".into()),
            key_count: 1,
        };
        let xml = serialize_list_bucket_result_v1("b", Some("x"), Some("m"), None, 1, &resp);
        assert!(xml.contains("<Prefix>x</Prefix>"));
        assert!(xml.contains("<Marker>m</Marker>"));
        assert!(!xml.contains("<NextMarker>"));
    }

    #[test]
    fn v1_not_truncated_has_no_next_marker() {
        let resp = ListObjectsResp {
            entries: vec![ObjectEntry {
                key: "a".into(),
                size: 1,
                etag: "e".into(),
                last_modified: 0,
            }],
            common_prefixes: vec!["p/".into()],
            is_truncated: false,
            next_continuation_token: None,
            key_count: 2,
        };
        let xml = serialize_list_bucket_result_v1("b", None, None, Some("/"), 10, &resp);
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml.contains("<NextMarker>"));
        assert!(xml.contains("<CommonPrefixes><Prefix>p/</Prefix></CommonPrefixes>"));
    }

    #[test]
    fn location_constraint_regions() {
        let us = serialize_location_constraint("us-east-1");
        assert!(
            us.contains("<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></LocationConstraint>"),
            "{us}"
        );
        let other = serialize_location_constraint("cn-north-1");
        assert!(other.contains("<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">cn-north-1</LocationConstraint>"), "{other}");
    }

    #[test]
    fn serialize_initiate_multipart_special_chars() {
        let xml = serialize_initiate_multipart("b", "key with <special> & chars", "id");
        assert!(xml.contains("key with &lt;special&gt; &amp; chars"));
    }

    #[test]
    fn serialize_complete_multipart_basic() {
        let xml = serialize_complete_multipart("mybucket", "obj.bin", "abc123-3");
        assert!(xml.contains("<Bucket>mybucket</Bucket>"));
        assert!(xml.contains("<Key>obj.bin</Key>"));
        assert!(xml.contains("<ETag>\"abc123-3\"</ETag>"));
    }

    #[test]
    fn serialize_list_parts_empty() {
        let resp = ListPartsResp {
            parts: vec![],
            is_truncated: false,
            next_part_number_marker: 0,
        };
        let xml = serialize_list_parts("uid-1", &resp);
        assert!(xml.contains("<UploadId>uid-1</UploadId>"));
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml.contains("<Part>"));
    }

    #[test]
    fn serialize_list_parts_with_entries() {
        let resp = ListPartsResp {
            parts: vec![
                PartEntry {
                    part_number: 1,
                    size: 5242880,
                    etag: "aaa".into(),
                },
                PartEntry {
                    part_number: 2,
                    size: 5242880,
                    etag: "bbb".into(),
                },
            ],
            is_truncated: true,
            next_part_number_marker: 2,
        };
        let xml = serialize_list_parts("uid-1", &resp);
        assert!(xml.contains("<PartNumber>1</PartNumber>"));
        assert!(xml.contains("<PartNumber>2</PartNumber>"));
        assert!(xml.contains("<Size>5242880</Size>"));
        assert!(xml.contains("<ETag>\"aaa\"</ETag>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<NextPartNumberMarker>2</NextPartNumberMarker>"));
    }

    #[test]
    fn serialize_list_uploads_empty() {
        let resp = ListUploadsResp {
            uploads: vec![],
            is_truncated: false,
            next_key_marker: None,
            next_upload_id_marker: None,
        };
        let xml = serialize_list_multipart_uploads("b", &resp);
        assert!(xml.contains("<Bucket>b</Bucket>"));
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml.contains("<Upload>"));
        assert!(!xml.contains("<NextKeyMarker>"));
    }

    #[test]
    fn serialize_list_uploads_with_entries() {
        let resp = ListUploadsResp {
            uploads: vec![UploadEntry {
                key: "file.bin".into(),
                upload_id: "uid-abc".into(),
                initiated: 1700000000000,
            }],
            is_truncated: true,
            next_key_marker: Some("file.bin".into()),
            next_upload_id_marker: Some("uid-abc".into()),
        };
        let xml = serialize_list_multipart_uploads("b", &resp);
        assert!(xml.contains("<Key>file.bin</Key>"));
        assert!(xml.contains("<UploadId>uid-abc</UploadId>"));
        assert!(xml.contains("<NextKeyMarker>file.bin</NextKeyMarker>"));
        assert!(xml.contains("<NextUploadIdMarker>uid-abc</NextUploadIdMarker>"));
    }

    #[test]
    fn serialize_delete_result_normal_mode() {
        let results = vec![
            DeleteResult {
                key: "a".into(),
                deleted: true,
                error: None,
            },
            DeleteResult {
                key: "b".into(),
                deleted: false,
                error: Some("boom".into()),
            },
        ];
        let xml = serialize_delete_result(&results, false);
        assert!(xml.contains("<Deleted><Key>a</Key></Deleted>"));
        assert!(xml.contains("<Error><Key>b</Key>"));
        assert!(xml.contains("<Message>boom</Message>"));
    }

    #[test]
    fn serialize_delete_result_quiet_mode() {
        let results = vec![
            DeleteResult {
                key: "a".into(),
                deleted: true,
                error: None,
            },
            DeleteResult {
                key: "b".into(),
                deleted: false,
                error: Some("boom".into()),
            },
        ];
        let xml = serialize_delete_result(&results, true);
        assert!(!xml.contains("<Deleted>"));
        assert!(xml.contains("<Error><Key>b</Key>"));
    }

    #[test]
    fn serialize_delete_result_empty() {
        let xml = serialize_delete_result(&[], false);
        assert!(xml.contains("<DeleteResult"));
        assert!(xml.contains("</DeleteResult>"));
    }

    #[test]
    fn copy_object_result_carries_quoted_etag_and_iso_date() {
        let xml = serialize_copy_object_result("abc123", 1_762_000_000_000);
        assert!(xml.contains("<CopyObjectResult"));
        assert!(xml.contains("<ETag>&quot;abc123&quot;</ETag>"));
        assert!(xml.contains("<LastModified>2025-11-01"));
    }

    #[test]
    fn object_attributes_emits_only_requested_members() {
        let out = ObjectAttributesOut {
            etag: Some("deadbee".into()),
            object_size: Some(42),
            storage_class: true,
        };
        let xml = serialize_object_attributes(&out);
        assert!(xml.contains("<ETag>deadbee</ETag>"));
        assert!(xml.contains("<ObjectSize>42</ObjectSize>"));
        assert!(xml.contains("<StorageClass>STANDARD</StorageClass>"));

        let sparse = ObjectAttributesOut {
            etag: None,
            object_size: Some(7),
            storage_class: false,
        };
        let xml = serialize_object_attributes(&sparse);
        assert!(!xml.contains("<ETag>"));
        assert!(!xml.contains("<StorageClass>"));
        assert!(xml.contains("<ObjectSize>7</ObjectSize>"));
    }

    #[test]
    fn epoch_to_iso_known_value() {
        let s = epoch_to_iso(1700000000000);
        assert_eq!(s, "2023-11-14T22:13:20.000Z");
    }

    #[test]
    fn epoch_to_iso_zero() {
        let s = epoch_to_iso(0);
        assert_eq!(s, "1970-01-01T00:00:00.000Z");
    }

    fn assert_valid_xml(xml: &str) {
        use quick_xml::events::Event;
        use quick_xml::Reader;
        let mut reader = Reader::from_str(xml);
        loop {
            match reader.read_event() {
                Ok(Event::Eof) => break,
                Err(e) => panic!(
                    "invalid XML at position {}: {e}\nXML: {xml}",
                    reader.error_position()
                ),
                _ => {}
            }
        }
    }

    #[test]
    fn all_serializers_produce_valid_xml() {
        assert_valid_xml(&serialize_list_all_my_buckets(&[]));
        assert_valid_xml(&serialize_list_all_my_buckets(&[BucketMeta {
            name: "test<&>".into(),
            owner: "o".into(),
            created_at: 0,
            acl: BucketAcl::Private,
        }]));

        let resp = ListObjectsResp {
            entries: vec![ObjectEntry {
                key: "k&v".into(),
                size: 1,
                etag: "e".into(),
                last_modified: 0,
            }],
            common_prefixes: vec!["p<>".into()],
            is_truncated: false,
            next_continuation_token: None,
            key_count: 2,
        };
        assert_valid_xml(&serialize_list_bucket_result(
            "b&b",
            Some("p<"),
            Some("/"),
            10,
            &resp,
        ));

        assert_valid_xml(&serialize_initiate_multipart("b", "k<>&", "id"));
        assert_valid_xml(&serialize_complete_multipart("b", "k", "etag"));

        let pr = ListPartsResp {
            parts: vec![],
            is_truncated: false,
            next_part_number_marker: 0,
        };
        assert_valid_xml(&serialize_list_parts("uid&", &pr));

        let ur = ListUploadsResp {
            uploads: vec![],
            is_truncated: false,
            next_key_marker: None,
            next_upload_id_marker: None,
        };
        assert_valid_xml(&serialize_list_multipart_uploads("b<>", &ur));

        let dr = vec![DeleteResult {
            key: "k&k".into(),
            deleted: true,
            error: None,
        }];
        assert_valid_xml(&serialize_delete_result(&dr, false));
        assert_valid_xml(&serialize_delete_result(&dr, true));
    }
}

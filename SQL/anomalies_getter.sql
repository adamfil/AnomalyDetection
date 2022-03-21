
SELECT *
FROM   (  
  SELECT /*+ index(dpa)*/ -- CBO sometimes wrongly bypasses the index
  data_payload_id, data_payload_uri,inserted_datetime,
  Utl_Compress.Lz_Uncompress (data_payload_content) as data_payload_content,
  author,dataset_id,is_archived, userenv_host, userenv_ip_address,
  userenv_os_user,userenv_client_identifier,uri_data_substr,
  data_input_datetime, data_processing_start_datetime,
  data_processing_end_datetime, instance_type, instance_datetime,
  instance_year,instance_month,instance_day,instance_hour,
  instance_minute,instance_second,received_datetime,
  latitude,longitude,area_coverage,station_identifier,original_header,
  null as compression_type,data_payload_type,
         COUNT(DISTINCT data_payload_uri) OVER (
           PARTITION BY SUBSTR(data_payload_uri, 1, INSTR(data_payload_uri, '/orig', 1))
         ) AS cnt
  FROM   dms_archive.data_payload_all dpa
  WHERE  data_payload_uri >= '/data/msc/observation/atmospheric/surface_weather/ca-1.1-ascii/decoded_qa_enhanced-xml-2.0/202101'
  AND    data_payload_uri <  '/data/msc/observation/atmospheric/surface_weather/ca-1.1-ascii/decoded_qa_enhanced-xml-2.0/202102'
  AND station_identifier in('8204402', '8205702', '8201390', '8203702', '8206491', '8204708')
)
WHERE  cnt > 1;

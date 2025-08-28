        insert into operator_contracts_agreements.t_format_c_commercial_discovery_document_details 
        (document_ref_number,document_type_id,document_name,format_c_id)
        SELECT 
        cf.logical_doc_id document_ref_number,
        cf.label_id document_type_id,
        cf.file_label document_name,
        a.format_c_id
        FROM dgh_staging.form_commerical_dis_format_c faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao.ava_with_contractor_fileref  = cmf.fileref 
        join operator_contracts_agreements.t_format_c_commercial_discovery_header a 
        on a.format_c_application_no = faao.refid 
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.fileref 
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
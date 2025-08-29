insert into operator_contracts_agreements.t_extension_production_sharing_contract(extension_production_sharing_contract_application_number,block_type,creation_date,block_id,awarded_under,block_name,consortium,psc_application_date,third_party_balance_reserve_dropdown,revised_fdp_dropdown,comments,declaration_checkbox,name_authorized_signatory,designation)
select
ref_id as extension_production_sharing_contract_application_number,
block_type as block_type,
created_on as creation_date,
block_id as block_id,
bid_round as awarded_under,
block_name as block_name,
consortium as consortium,
application_date as psc_application_date,
case
	when third_party_reserves_audit_report = true then '1'
	else '0'
end as third_party_balance_reserve_dropdown,
revised_field_development_plan as revised_fdp_dropdown,
comments as comments,
case
	when declaration = true then '1'
	else '0'
end as is_declared,
name_authorised_signatory_contractor as name_authorized_signatory,
designation as designation
from dgh_staging.temp_psc_extension_applications tpea
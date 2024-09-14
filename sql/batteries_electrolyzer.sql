DROP TABLE IF EXISTS stage.batteries_electrolyzer;
CREATE TABLE stage.batteries_electrolyzer AS
SELECT 
b.Est_cost_fbc_millions * e2.exchange_rate * g2.deflation_factor / e1.exchange_rate  / g1.deflation_factor as est_cost_fbc_millions_latest,
CASE WHEN b.act_duration_fbc is not NULL and b.est_duration_fbc is not NULL then (b.act_duration_fbc / b.est_duration_fbc)::float ELSE NULL END AS Schedule_Overrun_DOD_FBC_ratio,
CASE WHEN b.act_duration_construction is not NULL and b.est_duration_construction is not NULL THEN (b.act_duration_construction / b.est_duration_construction)::float ELSE NULL END AS Schedule_Overrun_construction_ratio,
b.*
FROM
(SELECT 
  a.Project_ID,
  a.Sample,
  a.Project_Name,
  a.Project_Class,
  a.Project_Type,
  a.Project_Subtype,
  a.Country_ISO3,
  c.name as country_name,
  c.subregion_name,
  a.Is_Complete,
  a.Construction_Company_Contractor,
  a.Organization_Name_Owner,
  a.Financing_Type,
  a.Organization_Name_Operator,
  
  a.start_Decision_to_Build_or_FBC_Year,
  a.start_Decision_to_Build_or_FBC_date,

  a.start_construction_date,
  a.start_construction_year,

  a.act_completion_year,
  a.act_completion_date,
  a.est_completion_date,

  a.act_construction_duration
  a.est_construction_duration
  a.act_Decision_to_Build_or_FBC_duration
  a.est_Decision_to_Build_or_FBC_duration


  a.Est_fbc_cost_local_Millions,
  a.Est_fbc_cost_local_Currency,
  a.Est_cost_local_Year,
  a.Act_cost_local_millions,
  a.Act_cost_local_currency,
  a.Act_cost_local_Year,
  a.New_Upgrade,
  a.Nameplate_Capacity_value,
  a.Nameplate_Capacity_units,
  a.Capacity_value,
  a.Capacity_Units,
  CASE WHEN a.completion_opening_date is not NULL and a.construction_start_date is not null THEN (a.completion_opening_date - a.construction_start_date)::float / 365 ELSE NULL END AS act_duration_construction,
  CASE WHEN a.completion_opening_date is not NULL and a.Decision_to_Build_or_FBC_date is not NULL THEN (a.completion_opening_date - a.decision_to_build_or_fbc_date)::float / 365 ELSE NULL END AS act_duration_fbc,
  CASE WHEN a.est_completion_fbc_date is not NULL and a.construction_start_date is not NULL THEN (a.est_completion_fbc_date - a.construction_start_date)::float / 365 ELSE NULL END AS est_duration_construction,
  CASE WHEN a.est_completion_fbc_date is not NULL and a.decision_to_Build_or_FBC_date is not NULL THEN (a.est_completion_fbc_date - a.decision_to_Build_or_FBC_date)::float / 365 ELSE NULL END AS est_duration_fbc
--   a.Project_Description,
--   a.Comments,
--   a.citations
FROM "raw"."batteries_electrolyzer" as a
LEFT JOIN "reference"."countries" as c 
ON country_iso3 = alpha3_code) as b
LEFT JOIN "reference"."exchange_rates" as e1 on (b.country_iso3 = e1.country_code) and (b.est_cost_fbc_year = e1.year)
LEFT JOIN "reference"."gdp_deflators" as g1 on (b.country_iso3 = g1.country_code) and (b.est_cost_fbc_year = g1.year)
LEFT JOIN (SELECT d1.* FROM "reference"."gdp_deflators" as d1 INNER JOIN (SELECT max(year) as year FROM "reference"."gdp_deflators") as d2 on d1.year = d2.year) as g2 on (b.country_iso3 = g2.country_code)
LEFT JOIN "reference"."exchange_rates" as e2 on (e2.country_code = 'USA') and (b.est_cost_fbc_year = e2.year)
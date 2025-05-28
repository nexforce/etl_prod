-- Nome da query: tl_dim_customer

-- CRIA TABELA COM CLIENTES EXISTENTES QUE SOFRERAM ALGUMA ATUALIZAÇÃO
CREATE OR REPLACE TABLE
   `starry-compiler-387319.Stage.stg_customer_need_updates`
OPTIONS(
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
     DIM.uuid
    ,DIM.external_id
    ,DIM.customer
    ,DIM.plan_points
    ,DIM.start_date
    ,DIM.due_date
    ,DIM.has_growth_formula
    ,DIM.slack_channel
    ,DIM.team
    ,DIM.date_created
    ,DIM.date_updated
    ,DIM.is_active_register
    ,DIM.status
    ,DIM.service_name -- nova coluna
    ,DIM.monthly_revenue -- nova coluna
    ,DIM.import_date
    ,DIM.import_date_time
FROM `starry-compiler-387319.Stage.stg_customer` AS STG
INNER JOIN `starry-compiler-387319.Clickup_Source.dim_customer` AS DIM
  ON DIM.service_name = STG.service_name --MUDOU PARA O SERVICE NAME
WHERE STG.customer IS NOT NULL
  AND DIM.is_active_register = true
  AND DIM.import_date <> STG.import_date;

-- ATUALIZA O REGISTRO ATUAL COMO OBSOLETO
UPDATE `starry-compiler-387319.Clickup_Source.dim_customer` AS DIM
SET DIM.is_active_register = false
FROM `starry-compiler-387319.Stage.stg_customer_need_updates` AS STG
WHERE DIM.uuid = STG.uuid;

-- INSERE O REGISTRO MAIS NOVO
INSERT INTO `starry-compiler-387319.Clickup_Source.dim_customer` (
   uuid
   ,external_id -- MUDAR PARA O EXTERNAL ID DE PURCHASED SERVICES? VERIFICAR DEPOIS
   ,customer
   ,plan_points
   ,start_date
   ,due_date
   ,has_growth_formula
   ,slack_channel
   ,team
   ,date_created
   ,date_updated
   ,is_active_register
   ,status
   ,service_name  -- nova coluna
   ,monthly_revenue -- nova coluna
   ,import_date
   ,import_date_time
) SELECT
      GENERATE_UUID() AS uuid
     ,STG.task_id
     ,STG.customer
     ,STG.plan_points
     ,STG.start_date
     ,STG.due_date
     ,IFNULL(STG.has_growth_formula, false) AS has_growth_formula
     ,STG.slack_channel
     ,STG.team
     ,STG.date_created
     ,STG.date_updated
     ,true AS is_active_register
     ,INITCAP(STG.status) AS status
     ,STG.service_name  -- nova coluna
     ,STG.monthly_revenue -- nova coluna
     ,STG.import_date
     ,STG.import_date_time
FROM `starry-compiler-387319.Stage.stg_customer` AS STG
INNER JOIN `starry-compiler-387319.Stage.stg_customer_need_updates` AS UPD
  ON UPD.service_name = STG.service_name; --MUDOU PARA O SERVICE NAME
-- CRIA TABELA COM OS CLIENTES NOVOS QUE NÃO EXISTEM NA BASE
CREATE OR REPLACE TABLE
   `starry-compiler-387319.Stage.stg_customer_new`
OPTIONS(
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
     STG.task_id
    ,STG.customer
    ,STG.plan_points AS plan_points
    ,STG.start_date
    ,STG.due_date
    ,IFNULL(STG.has_growth_formula, false) AS has_growth_formula
    ,STG.slack_channel
    ,STG.team
    ,STG.date_created
    ,STG.date_updated
    ,true AS is_active_register
    ,INITCAP(STG.status) AS status
    ,STG.service_name -- nova coluna
    ,STG.monthly_revenue -- nova coluna
    ,STG.import_date
    ,STG.import_date_time
FROM `starry-compiler-387319.Stage.stg_customer` AS STG
LEFT JOIN `starry-compiler-387319.Clickup_Source.dim_customer` AS DIM
  ON DIM.external_id = STG.task_id --VERIFICAR SE PODE DAR MERDA NO FUTURO
WHERE STG.service_name IS NOT NULL -- MUDOU PARA O SERVICE NAME
  AND DIM.service_name IS NULL;
-- INSERE O REGISTRO NOVO NA BASE
INSERT INTO `starry-compiler-387319.Clickup_Source.dim_customer` (
     uuid
    ,external_id
    ,customer
    ,plan_points
    ,start_date
    ,due_date
    ,has_growth_formula
    ,slack_channel
    ,team
    ,date_created
    ,date_updated
    ,is_active_register
    ,status
    ,service_name  -- nova coluna
    ,monthly_revenue -- nova coluna
    ,import_date
    ,import_date_time
) SELECT
     GENERATE_UUID() AS uuid
    ,STG.task_id
    ,STG.customer
    ,STG.plan_points AS plan_points
    ,STG.start_date
    ,STG.due_date
    ,IFNULL(STG.has_growth_formula, false) AS has_growth_formula
    ,STG.slack_channel
    ,STG.team
    ,STG.date_created
    ,STG.date_updated
    ,true AS is_active_register
    ,INITCAP(STG.status) AS status
    ,STG.service_name  -- nova coluna
    ,STG.monthly_revenue -- nova coluna
    ,STG.import_date
    ,STG.import_date_time
FROM `starry-compiler-387319.Stage.stg_customer_new` AS STG;

-- Nome da query: tl_dim_squad

--CRIA TABELA COM SQUAD EXISTENTES QUE SOFRERAM ALGUMA ATUALIZAÇÃO
CREATE OR REPLACE TABLE 
   `starry-compiler-387319.Stage.stg_squad_need_updates`
OPTIONS(
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS 
SELECT
     DIM.uuid
    ,DIM.external_id
    ,TRIM(DIM.member_name) AS member_name         
    ,DIM.email             
    ,DIM.position          
    ,DIM.team              
    ,DIM.capacity_points   
    ,DIM.monthly_cost      
    ,DIM.slack_channel     
    ,DIM.status            
    ,DIM.list              
    ,DIM.start_date        
    ,DIM.due_date          
    ,DIM.date_created 
    ,DIM.date_updated      
    ,DIM.is_active_register
    ,DIM.import_date
    ,DIM.import_date_time
FROM `starry-compiler-387319.Stage.stg_squad` AS STG
INNER JOIN `starry-compiler-387319.Clickup_Source.dim_squad` AS DIM ON DIM.email = STG.email
WHERE STG.task_name IS NOT NULL
  AND DIM.is_active_register = true
  AND DIM.import_date <> STG.import_date;

--ATUALIZA O REGISTRO ATUAL COMO OBSOLETO
UPDATE `starry-compiler-387319.Clickup_Source.dim_squad` AS DIM
SET DIM.is_active_register = false
FROM `starry-compiler-387319.Stage.stg_squad_need_updates` AS STG
WHERE DIM.uuid = STG.uuid;


--INSERE O REGISTRO MAIS NOVO
INSERT INTO `starry-compiler-387319.Clickup_Source.dim_squad` (
     uuid
    ,external_id
    ,member_name         
    ,email             
    ,position          
    ,team              
    ,capacity_points   
    ,monthly_cost      
    ,slack_channel     
    ,status            
    ,list              
    ,start_date        
    ,due_date          
    ,date_created      
    ,date_updated      
    ,is_active_register
    ,import_date
    ,import_date_time
  ) SELECT
     GENERATE_UUID()
    ,STG.task_id
    ,TRIM(STG.task_name) AS task_name         
    ,STG.email             
    ,STG.position          
    ,STG.team              
    ,STG.capacity_points   
    ,STG.monthly_cost      
    ,STG.slack_channel     
    ,INITCAP(STG.status)            
    ,STG.list              
    ,PARSE_DATE('%d/%m/%Y', NULLIF(STG.start_date, 'None')) AS start_date      
    ,PARSE_DATE('%d/%m/%Y', NULLIF(STG.due_date, 'None')) AS due_date           
    ,STG.date_created      
    ,STG.date_updated   
    ,true
    ,STG.import_date
    ,STG.import_date_time
FROM `starry-compiler-387319.Stage.stg_squad` AS STG 
    INNER JOIN `starry-compiler-387319.Stage.stg_squad_need_updates` AS UPD ON UPD.email = STG.email;

-------------------------------------------------------------------------------------------------------------------

--CRIA TABELA COM OS SQUADS NOVOS QUE NÃO EXISTEM NA BASE
CREATE OR REPLACE TABLE 
   `starry-compiler-387319.Stage.stg_squad_new`
OPTIONS(
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS 
SELECT
     STG.task_id
    ,TRIM(STG.task_name) AS task_name         
    ,STG.email             
    ,STG.position          
    ,STG.team              
    ,STG.capacity_points   
    ,STG.monthly_cost      
    ,STG.slack_channel     
    ,INITCAP(STG.status) AS status       
    ,STG.list              
    ,PARSE_DATE('%d/%m/%Y', NULLIF(STG.start_date, 'None')) AS start_date      
    ,PARSE_DATE('%d/%m/%Y', NULLIF(STG.due_date, 'None')) AS due_date        
    ,STG.date_created      
    ,STG.date_updated
    ,STG.import_date
    ,STG.import_date_time
FROM `starry-compiler-387319.Stage.stg_squad` AS STG
LEFT JOIN `starry-compiler-387319.Clickup_Source.dim_squad` AS DIM ON DIM.email = STG.email
WHERE STG.task_name IS NOT NULL
  AND DIM.member_name IS NULL;


--INSERE O REGISTRO MAIS NOVO
INSERT INTO `starry-compiler-387319.Clickup_Source.dim_squad` (
     uuid
    ,external_id
    ,member_name         
    ,email             
    ,position          
    ,team              
    ,capacity_points   
    ,monthly_cost      
    ,slack_channel     
    ,status            
    ,list              
    ,start_date        
    ,due_date          
    ,date_created      
    ,date_updated      
    ,is_active_register
    ,import_date
    ,import_date_time
  ) SELECT
     GENERATE_UUID()
    ,STG.task_id
    ,TRIM(STG.task_name) AS task_name     
    ,STG.email             
    ,STG.position          
    ,STG.team              
    ,STG.capacity_points   
    ,STG.monthly_cost      
    ,STG.slack_channel     
    ,INITCAP(STG.status)            
    ,STG.list              
    ,STG.start_date       
    ,STG.due_date     
    ,STG.date_created      
    ,STG.date_updated
    ,true
    ,STG.import_date
    ,STG.import_date_time
 FROM `starry-compiler-387319.Stage.stg_squad_new` AS STG; 

-- Nome da query: tl_dim_task

-- CRIA TABELA COM TAREFAS EXISTENTES QUE SOFRERAM ALGUMA ATUALIZAÇÃO
CREATE OR REPLACE TABLE 
   `starry-compiler-387319.Stage.stg_task_need_updates`
OPTIONS(
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS 
SELECT
     DIM.uuid
    ,STG.task_id
    ,STG.task_name
    ,STG.sprint
    ,STG.customer
    ,STG.team
    ,TRIM(STG.assignee) AS assignee
    ,IFNULL(STG.reviewer,'Undefined') AS reviewer
    ,INITCAP(STG.priority) AS priority
    ,INITCAP(STG.category) AS category
    ,INITCAP(STG.status) AS status
    ,STG.list
    ,IFNULL(STG.points_estimate,0) AS points_estimate
    ,IFNULL(STG.points_estimate_rolled_up,0) AS points_estimate_rolled_up
    ,STG.start_date
    ,STG.due_date
    ,STG.date_closed
    ,STG.date_done
    ,STG.date_created
    ,STG.date_updated
    ,TRUE AS is_active_register
    ,SAFE_CAST(STG.quality_score AS INT64) AS quality_score
    ,STG.email AS assignee_email
    ,STG.import_date -- ADICIONANDO A COLUNA IMPORT_DATE
    ,STG.import_date_time
FROM `starry-compiler-387319.Stage.stg_task` AS STG
INNER JOIN `starry-compiler-387319.Clickup_Source.dim_task` AS DIM 
  ON DIM.task_id = STG.task_id
WHERE STG.task_name IS NOT NULL
  AND DIM.task_name IS NOT NULL
  AND DIM.is_active_register = TRUE
  AND DIM.import_date <> STG.import_date;

-- ATUALIZA O REGISTRO ATUAL COMO OBSOLETO
UPDATE `starry-compiler-387319.Clickup_Source.dim_task` AS DIM
SET DIM.is_active_register = FALSE
FROM `starry-compiler-387319.Stage.stg_task_need_updates` AS STG
WHERE DIM.uuid = STG.uuid;

-- INSERE O REGISTRO MAIS NOVO
INSERT INTO `starry-compiler-387319.Clickup_Source.dim_task` (
     uuid
    ,task_id
    ,task_name
    ,sprint
    ,customer
    ,team
    ,assignee
    ,reviewer
    ,priority
    ,category
    ,status
    ,list
    ,points_estimate
    ,points_estimated_rolled_up
    ,start_date
    ,due_date
    ,date_closed
    ,date_done
    ,date_created
    ,date_updated
    ,is_active_register
    ,quality_score
    ,assignee_email
    ,import_date -- ADICIONANDO A COLUNA IMPORT_DATE
    ,import_date_time
) 
SELECT
      GENERATE_UUID()
      ,STG.task_id
      ,STG.task_name
      ,STG.sprint
      ,STG.customer
      ,STG.team
      ,TRIM(STG.assignee) 
      ,IFNULL(STG.reviewer,'Undefined')
      ,INITCAP(STG.priority)
      ,INITCAP(STG.category)
      ,INITCAP(STG.status)
      ,STG.list
      ,IFNULL(STG.points_estimate,0)
      ,IFNULL(STG.points_estimate_rolled_up,0)
      ,STG.start_date
      ,STG.due_date
      ,STG.date_closed
      ,STG.date_done
      ,STG.date_created
      ,STG.date_updated
      ,TRUE AS is_active_register
      ,SAFE_CAST(STG.quality_score AS INT64) AS quality_score
      ,STG.assignee_email
      ,STG.import_date -- IMPORTANDO A DATA DA STAGE PARA A DIM
      ,STG.import_date_time
FROM `starry-compiler-387319.Stage.stg_task_need_updates` AS STG;

-- CRIA TABELA COM AS TAREFAS QUE NÃO EXISTEM NA BASE
CREATE OR REPLACE TABLE 
   `starry-compiler-387319.Stage.stg_task_new`
OPTIONS(
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS 
SELECT
     STG.task_id
    ,STG.task_name
    ,STG.sprint
    ,STG.customer
    ,STG.team
    ,TRIM(STG.assignee) AS assignee
    ,IFNULL(STG.reviewer,'Undefined') AS reviewer
    ,INITCAP(STG.priority) AS priority
    ,INITCAP(STG.category) AS category
    ,INITCAP(STG.status) AS status
    ,STG.list
    ,IFNULL(STG.points_estimate,0) AS points_estimate
    ,IFNULL(STG.points_estimate_rolled_up,0) AS points_estimate_rolled_up
    ,STG.start_date
    ,STG.due_date
    ,STG.date_closed
    ,STG.date_done
    ,STG.date_created
    ,STG.date_updated
    ,TRUE AS is_active_register
    ,SAFE_CAST(STG.quality_score AS INT64) AS quality_score
    ,STG.email AS assignee_email
    ,STG.import_date -- IMPORTANDO A DATA DA STAGE
    ,STG.import_date_time
FROM `starry-compiler-387319.Stage.stg_task` AS STG
LEFT JOIN `starry-compiler-387319.Clickup_Source.dim_task` AS DIM 
  ON DIM.task_id = STG.task_id
WHERE STG.task_name IS NOT NULL
  AND DIM.task_name IS NULL;

-- INSERE O REGISTRO NOVO
INSERT INTO `starry-compiler-387319.Clickup_Source.dim_task` (
     uuid
    ,task_id
    ,task_name
    ,sprint
    ,customer
    ,team
    ,assignee
    ,reviewer
    ,priority
    ,category
    ,status
    ,list
    ,points_estimate
    ,points_estimated_rolled_up
    ,start_date
    ,due_date
    ,date_closed
    ,date_done
    ,date_created
    ,date_updated
    ,is_active_register
    ,quality_score
    ,assignee_email
    ,import_date -- ADICIONANDO A COLUNA IMPORT_DATE
    ,import_date_time
) 
SELECT
      GENERATE_UUID()
      ,STG.task_id
      ,STG.task_name
      ,STG.sprint
      ,STG.customer
      ,STG.team
      ,TRIM(STG.assignee) 
      ,IFNULL(STG.reviewer,'Undefined')
      ,INITCAP(STG.priority)
      ,INITCAP(STG.category)
      ,INITCAP(STG.status)
      ,STG.list
      ,IFNULL(STG.points_estimate,0)
      ,IFNULL(STG.points_estimate_rolled_up,0)
      ,STG.start_date
      ,STG.due_date
      ,STG.date_closed
      ,STG.date_done
      ,STG.date_created
      ,STG.date_updated
      ,TRUE AS is_active_register
      ,SAFE_CAST(STG.quality_score AS INT64) AS quality_score
      ,STG.assignee_email
      ,STG.import_date -- IMPORTANDO A DATA DA STAGE PARA A DIM
      ,STG.import_date_time
FROM `starry-compiler-387319.Stage.stg_task_new` AS STG;


-- Nome da query: tl_fat_sprint_detail

--CRIA TABELA COM OS REGISTROS CLIENTES EXISTENTES QUE SOFRERAM ALGUMA ATUALIZAÇÃO
CREATE OR REPLACE TABLE
   `starry-compiler-387319.Stage.stg_fat_new`
OPTIONS(
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
   SPR.uuid AS sprint_uuid
  ,TAS.uuid AS task_uuid
  ,CUS.uuid AS customer_uuid
  ,SQD.uuid AS squad_uuid
  ,TAS.points_estimate
  ,TAS.points_estimated_rolled_up
  ,SUM(CASE WHEN TAS.status = "Done" THEN TAS.points_estimate ELSE 0 END) AS points_done
FROM
  `starry-compiler-387319.Clickup_Source.dim_task` AS TAS
  INNER JOIN `starry-compiler-387319.Clickup_Source.dim_sprint` AS SPR ON SPR.sprint = CONCAT('2025 - ', TAS.sprint) --SUBSTITUIR PARA O ANO CORRESPONDENTE
  INNER JOIN `starry-compiler-387319.Clickup_Source.dim_customer` AS CUS ON TAS.customer = CUS.service_name --ALTERAR PARA SERVICE NAME
  INNER JOIN `starry-compiler-387319.Clickup_Source.dim_squad` AS SQD ON TAS.assignee_email = SQD.email --GARANTIR QUE FUNCIONOU!!!!!!!!!
WHERE
  TAS.uuid NOT IN(SELECT task_uuid from `starry-compiler-387319.Clickup_Source.fat_sprint_detail`)
  AND CUS.is_active_register = true
  AND SQD.is_active_register = true
  AND TAS.is_active_register = true
GROUP BY
   SPR.uuid
  ,TAS.uuid
  ,CUS.uuid
  ,SQD.uuid
  ,TAS.points_estimate
  ,TAS.points_estimated_rolled_up
ORDER BY
  SQD.uuid ASC;

-- INSERE OS REGISTROS NA TABELA FAT_SPRINT_DETAIL
INSERT INTO `starry-compiler-387319.Clickup_Source.fat_sprint_detail`
SELECT
   GENERATE_UUID() AS uuid
  ,STG.sprint_uuid 
  ,STG.task_uuid
  ,STG.customer_uuid
  ,STG.squad_uuid
  ,STG.points_estimate
  ,STG.points_estimated_rolled_up
  ,STG.points_done
FROM
  `starry-compiler-387319.Stage.stg_fat_new` AS STG 

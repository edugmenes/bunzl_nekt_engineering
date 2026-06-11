# Bunzl Pipedrive — Bronze Table Schemas

All tables below live in the **Bronze** layer of the Nekt platform and are prefixed with `bunzl_pipedrive_bronze_`.

---

## bunzl_pipedrive_bronze_deals

Primary source for the silver ETL. One row per deal.

| Column | Type | Notes |
|---|---|---|
| id | integer | Primary key |
| title | string | Deal title |
| creator_user_id | integer | |
| owner_id | integer | FK → users.id |
| value | integer | Deal value |
| person_id | integer | FK → persons.id |
| org_id | integer | FK → organizations.id |
| stage_id | integer | FK → stages.id |
| pipeline_id | integer | FK → pipelines.id |
| currency | string | |
| add_time | timestamp | Date the lead/deal was added |
| update_time | timestamp | |
| stage_change_time | timestamp | |
| status | string | open / won / lost / deleted |
| is_deleted | boolean | Soft-delete flag |
| probability | integer | |
| lost_reason | string | |
| visible_to | integer | |
| close_time | timestamp | |
| won_time | timestamp | |
| lost_time | timestamp | |
| local_won_date | timestamp | |
| local_lost_date | timestamp | |
| local_close_date | timestamp | |
| expected_close_date | timestamp | |
| label_ids | array | |
| origin | string | |
| origin_id | integer | |
| channel | integer | |
| channel_id | string | |
| acv | integer | |
| arr | integer | |
| mrr | integer | |
| next_activity_id | integer | |
| last_activity_id | integer | |
| first_won_time | timestamp | |
| products_count | integer | |
| files_count | integer | |
| notes_count | integer | |
| followers_count | integer | |
| email_messages_count | integer | |
| activities_count | integer | |
| done_activities_count | integer | |
| undone_activities_count | integer | |
| participants_count | integer | |
| last_incoming_mail_time | timestamp | |
| last_outgoing_mail_time | timestamp | |
| custom_fields | object | Nested struct with deal-level custom fields |

---

## bunzl_pipedrive_bronze_organizations

One row per organization (company/client).

| Column | Type | Notes |
|---|---|---|
| id | integer | Primary key |
| name | string | Organization name |
| owner_id | integer | FK → users.id |
| org_id | integer | |
| add_time | timestamp | |
| update_time | timestamp | |
| address | object | Nested struct: route, value, country, locality, postal_code, sublocality, street_number, formatted_address, admin_area_level_1, admin_area_level_2 |
| is_deleted | boolean | |
| visible_to | integer | |
| label_ids | array | |
| custom_fields | object | Nested struct with org-level custom fields (see field mapping below) |

### custom_fields field mapping (organization_fields id 26)

| Hex Key | Field Name | Type | Notes |
|---|---|---|---|
| e1897931095ea6e1bba9b0ebdecfb6ad7587ec27 | CNPJ | string | organization_fields.id = 26 |

---

## bunzl_pipedrive_bronze_pipelines

Lookup table for pipeline names.

| Column | Type |
|---|---|
| id | integer |
| name | string |
| order_nr | integer |
| is_deleted | boolean |
| is_deal_probability_enabled | boolean |
| add_time | timestamp |
| update_time | timestamp |
| selected | boolean |

---

## bunzl_pipedrive_bronze_stages

Lookup table for stage names (belongs to a pipeline).

| Column | Type |
|---|---|
| id | integer |
| order_nr | integer |
| name | string |
| is_deleted | boolean |
| deal_probability | integer |
| pipeline_id | integer |
| is_deal_rot_enabled | boolean |
| days_to_rotten | integer |
| add_time | timestamp |
| update_time | timestamp |

---

## bunzl_pipedrive_bronze_users

Lookup table for user/owner names and emails.

| Column | Type |
|---|---|
| id | integer |
| name | string |
| email | string |
| phone | string |
| lang | integer |
| locale | string |
| timezone_name | string |
| timezone_offset | string |
| default_currency | string |
| icon_url | string |
| active_flag | boolean |
| is_admin | integer |
| role_id | integer |
| created | timestamp |
| modified | timestamp |
| last_login | timestamp |
| has_created_company | boolean |
| is_you | boolean |
| is_deleted | boolean |
| access | array\<object\> |

---

## bunzl_pipedrive_bronze_activities

Activity log per deal/org/person. Not used in current silver ETL.

Key columns: `id`, `deal_id`, `org_id`, `person_id`, `user_id`, `type`, `done`, `add_time`, `due_date`, `subject`.

---

## bunzl_pipedrive_bronze_notes

Notes attached to deals/orgs/persons. Not used in current silver ETL.

Key columns: `id`, `deal_id`, `org_id`, `person_id`, `user_id`, `content`, `add_time`.

---

## bunzl_pipedrive_bronze_persons

Contact persons. Not used in current silver ETL.

Key columns: `id`, `name`, `first_name`, `last_name`, `org_id`, `owner_id`, `emails`, `phones`, `add_time`.

---

## bunzl_pipedrive_bronze_deal_fields

Metadata table mapping custom field hex keys to human-readable names for deals.
Key columns: `id`, `key` (hex), `name`, `field_type`.

---

## bunzl_pipedrive_bronze_organization_fields

Metadata table mapping custom field hex keys to human-readable names for organizations.
Key columns: `id`, `key` (hex), `name`, `field_type`.

| id | key | name |
|---|---|---|
| 26 | e1897931095ea6e1bba9b0ebdecfb6ad7587ec27 | CNPJ |

---

## bunzl_pipedrive_bronze_person_fields

Metadata table mapping custom field hex keys to human-readable names for persons. Not used in current silver ETL.

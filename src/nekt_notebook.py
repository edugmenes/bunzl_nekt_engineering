## IMPORTS
import nekt
from typing         import Optional
from pyspark.sql    import Window
from pyspark.sql    import DataFrame, Column
from pyspark.sql    import functions as F

## HELPER FUNCTIONS
def extract_nekt_table(layer_name: str, table_name: str) -> DataFrame:
    """Simplify nekt table extraction syntax."""
    return nekt.load_table(layer_name=layer_name, table_name=table_name)

def save_nekt_table(
    df: DataFrame,
    layer_name: str,
    table_name: str,
    folder_name: Optional[str] = None
):
    """Simplify nekt table saving syntax."""
    nekt.save_table(
        df=df,
        layer_name=layer_name,
        table_name=table_name,
        folder_name=folder_name
    )

def ms_to_timestamp(col_name: str) -> Column:
    """Converts a Unix millisecond epoch column to a TimestampType."""
    return F.to_timestamp(F.col(col_name).cast("long") / 1000)

def last_element(array_col: str, field: str) -> F.Column:
    """Safely retrieves a field from the last element of an array column."""
    return (
        F.when(
            F.size(F.col(array_col)) > 0,
            F.element_at(F.col(array_col), F.size(F.col(array_col)))[field]
        ).otherwise(F.lit(None))
    )

## EXTRACTING TABLES
# pipedrive - bronze tables
df_bronze_deals         = extract_nekt_table("Bronze", "bunzl_pipedrive_bronze_deals")
df_bronze_organizations = extract_nekt_table("Bronze", "bunzl_pipedrive_bronze_organizations")
df_bronze_pipelines     = extract_nekt_table("Bronze", "bunzl_pipedrive_bronze_pipelines")
df_bronze_stages        = extract_nekt_table("Bronze", "bunzl_pipedrive_bronze_stages")
df_bronze_users         = extract_nekt_table("Bronze", "bunzl_pipedrive_bronze_users")

## TRANSFORMING TABLES
# pipedrive - silver data (deals enriched with org, pipeline, stage, owner)
df_silver_pipedrive_data = (
    df_bronze_deals.alias("d")
    .filter(
        F.col("d.id").isNotNull() &
        F.col("d.add_time").isNotNull() &
        (F.col("d.is_deleted") == False)
    )
    .join(
        df_bronze_organizations.alias("o"),
        on=F.col("d.org_id") == F.col("o.id"),
        how="left",
    )
    .join(
        df_bronze_pipelines.alias("p"),
        on=F.col("d.pipeline_id") == F.col("p.id"),
        how="left",
    )
    .join(
        df_bronze_stages.alias("s"),
        on=F.col("d.stage_id") == F.col("s.id"),
        how="left",
    )
    .join(
        df_bronze_users.alias("u"),
        on=F.col("d.owner_id") == F.col("u.id"),
        how="left",
    )
    .select(
        # deal - identifiers
        F.col("d.id")                        .cast("integer").alias("deal_id"),
        F.col("d.add_time")                  .cast("string") .alias("deal_add_time"),
        F.col("d.origin")                    .cast("string") .alias("deal_origin"),
        F.col("d.origin_id")                 .cast("integer").alias("deal_origin_id"),
        F.col("d.channel")                   .cast("integer").alias("deal_channel"),
        F.col("d.channel_id")                .cast("string") .alias("deal_channel_id"),
        # deal - info
        F.col("d.title")                     .cast("string") .alias("deal_title"),
        F.col("d.value")                     .cast("integer").alias("deal_value"),
        F.col("d.currency")                  .cast("string") .alias("deal_currency"),
        F.col("d.status")                    .cast("string") .alias("deal_status"),
        F.col("d.lost_reason")               .cast("string") .alias("deal_lost_reason"),
        F.col("d.probability")               .cast("integer").alias("deal_probability"),
        F.col("d.is_deleted")                .cast("boolean").alias("deal_is_deleted"),
        # deal - dates
        F.col("d.update_time")               .cast("string") .alias("deal_update_time"),
        F.col("d.stage_change_time")         .cast("string") .alias("deal_stage_change_time"),
        F.col("d.expected_close_date")       .cast("string") .alias("deal_expected_close_date"),
        F.col("d.close_time")                .cast("string") .alias("deal_close_time"),
        F.col("d.won_time")                  .cast("string") .alias("deal_won_time"),
        F.col("d.lost_time")                 .cast("string") .alias("deal_lost_time"),
        # deal - activity counts
        F.col("d.activities_count")          .cast("integer").alias("deal_activities_count"),
        F.col("d.done_activities_count")     .cast("integer").alias("deal_done_activities_count"),
        F.col("d.undone_activities_count")   .cast("integer").alias("deal_undone_activities_count"),
        # pipeline
        F.col("d.pipeline_id")               .cast("integer").alias("pipeline_id"),
        F.col("p.name")                      .cast("string") .alias("pipeline_name"),
        # stage
        F.col("d.stage_id")                  .cast("integer").alias("stage_id"),
        F.col("s.name")                      .cast("string") .alias("stage_name"),
        # owner
        F.col("d.owner_id")                  .cast("integer").alias("owner_id"),
        F.col("u.name")                      .cast("string") .alias("owner_name"),
        F.col("u.email")                     .cast("string") .alias("owner_email"),
        # organization - identifiers
        F.col("d.org_id")                    .cast("integer").alias("org_id"),
        F.col("o.name")                      .cast("string") .alias("org_name"),
        F.col("o.custom_fields").getField("e1897931095ea6e1bba9b0ebdecfb6ad7587ec27").cast("string").alias("org_cnpj"),
        # organization - address
        F.col("o.address.value")             .cast("string") .alias("org_address"),
        F.col("o.address.route")             .cast("string") .alias("org_address_route"),
        F.col("o.address.street_number")     .cast("string") .alias("org_address_street_number"),
        F.col("o.address.sublocality")       .cast("string") .alias("org_address_sublocality"),
        F.col("o.address.locality")          .cast("string") .alias("org_address_locality"),
        F.col("o.address.admin_area_level_1").cast("string") .alias("org_address_state"),
        F.col("o.address.admin_area_level_2").cast("string") .alias("org_address_city"),
        F.col("o.address.country")           .cast("string") .alias("org_address_country"),
        F.col("o.address.postal_code")       .cast("string") .alias("org_address_postal_code"),
        # audit
        F.current_timestamp()                .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["deal_id"]
    )
)

## LOADING TABLES
# pipedrive - silver tables
save_nekt_table(df_silver_pipedrive_data, "Silver", "bunzl_pipedrive_silver_data", "bunzl_pipedrive_silver")

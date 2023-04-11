import logging
import typing

import apache_beam as beam
from apache_beam.transforms.sql import SqlTransform


class Dataset1Dict(beam.DoFn):
    def process(self, elem):
        try:
            row = elem.split(",")
            data = {
                "invoice_id": int(row[0]),
                "legal_entity": str(row[1]),
                "counter_party": str(row[2]),
                "rating": int(row[3]),
                "status": str(row[4]),
                "raw_value": int(row[5]),
            }

            yield data
        except Exception:
            print(f"Parse error on {elem}")


class Dataset2Dict(beam.DoFn):
    def process(self, elem):
        try:
            row = elem.split(",")
            data = {
                "counter_party": str(row[0]),
                "tier": int(row[1])
            }

            yield data
        except Exception:
            print(f"Parse error on {elem}")


class Dataset1Schema(typing.NamedTuple):
    invoice_id: int
    legal_entity: str
    counter_party: str
    rating: int
    status: str
    raw_value: int


class Dataset2Schema(typing.NamedTuple):
    counter_party: str
    tier: int


class BeamSolution:
    def __init__(self):
        pass

    def run_pipeline(self):
        beam.coders.registry.register_coder(Dataset1Schema, beam.coders.RowCoder)
        beam.coders.registry.register_coder(Dataset2Schema, beam.coders.RowCoder)

        with beam.Pipeline() as p:
            invoice_lines = p | "Read invoices" >> beam.io.ReadFromText("./dataset1.csv", skip_header_lines=1, delimiter=str.encode("\r"))
            counter_party_lines = p | "Read parties" >> beam.io.ReadFromText("./dataset2.csv", skip_header_lines=1, delimiter=str.encode("\r"))

            invoice_collection = (
                invoice_lines
                    | "Parse invoice lines" >> beam.ParDo(Dataset1Dict())
                    | "Map invoice to Row" >> beam.Map(lambda x: Dataset1Schema(**x)).with_output_types(Dataset1Schema)
            )

            counter_party_collection = (
                counter_party_lines
                    | "Parse party lines" >> beam.ParDo(Dataset2Dict())
                    | "Map party to row" >> beam.Map(lambda x: Dataset2Schema(**x)).with_output_types(Dataset2Schema)
            )

            tuple_tags = {"invoice": invoice_collection, "counter_party": counter_party_collection}

            (
                tuple_tags
                | SqlTransform(
                    """
                    SELECT 
                        COALESCE(joined_tbl.legal_entity, 'Total') AS legal_entity, 
                        COALESCE(joined_tbl.counter_party, 'Total') AS counterparty, 
                        COALESCE(joined_tbl.tier, 'Total') AS tier, 
                        MAX(joined_tbl.rating) AS max_rating,
                        SUM(CASE WHEN joined_tbl.status = 'ARAP' THEN joined_tbl.raw_value ELSE 0 END) AS sum_arap_val,
                        SUM(CASE WHEN joined_tbl.status = 'ACCR' THEN joined_tbl.raw_value ELSE 0 END) AS sum_accr_val
                    FROM
                    (
                        SELECT i.invoice_id, i.legal_entity, i.counter_party, i.rating, i.status, i.raw_value, cp.tier
                        FROM invoice i
                        LEFT JOIN counter_party cp ON (i.counter_party = cp.counter_party)
                    ) AS joined_tbl
                    GROUP BY joined_tbl.legal_entity, joined_tbl.counter_party, joined_tbl.tier
                    ---GROUP BY CUBE(joined_tbl.legal_entity, joined_tbl.counter_party, joined_tbl.tier)
                    /*
                    NOTE: GROUP BY CUBE(...) would allow us to group by all unique combinations of (legal_entity, counter_party, and tier)
                    However, it appears to not be behaving correctly.
                    
                    I even tried the following, with no luck.
                    GROUP BY
                        GROUPING SETS (
                        (joined_tbl.legal_entity, joined_tbl.counter_party, joined_tbl.tier), 
                        (joined_tbl.legal_entity, joined_tbl.counter_party),
                        (joined_tbl.legal_entity, joined_tbl.tier),
                        (joined_tbl.counter_party, joined_tbl.tier),
                        (joined_tbl.legal_entity),
                        (joined_tbl.counter_party),
                        (joined_tbl.tier),
                        ()
                    )
                    
                    I receive an error stating: RuntimeError: org.apache.beam.sdk.extensions.sql.impl.SqlConversionException: Unable to convert query 
                    
                    With more context given:
                    Caused by: org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelOptPlanner$CannotPlanException: There are not enough rules to produce a node with desired properties: convention=BEAM_LOGICAL.
                    Missing conversion is LogicalAggregate[convention: NONE -> BEAM_LOGICAL]
                    There is 1 empty subset: rel#45:RelSubset#4.BEAM_LOGICAL, the relevant part of the original plan is as follows
                    22:LogicalAggregate(group=[{0, 1, 2}], groups=[[{0, 1, 2}, {0, 1}, {0, 2}, {0}, {1, 2}, {1}, {2}, {}]], max_rating=[MAX($3)], sum_arap_val=[SUM($4)], sum_accr_val=[SUM($5)])
                        20:LogicalProject(subset=[rel#21:RelSubset#3.NONE], legal_entity=[$1], counter_party=[$2], tier=[$7], rating=[$3], $f4=[CASE(=($4, 'ARAP'), $5, 0:BIGINT)], $f5=[CASE(=($4, 'ACCR'), $5, 0:BIGINT)])
                            18:LogicalJoin(subset=[rel#19:RelSubset#2.NONE], condition=[=($2, $6)], joinType=[left])
                                10:BeamIOSourceRel(subset=[rel#16:RelSubset#0.BEAM_LOGICAL], table=[[beam, invoice]])
                                11:BeamIOSourceRel(subset=[rel#17:RelSubset#1.BEAM_LOGICAL], table=[[beam, counter_party]])
                    */
                    """,
                )
                | beam.Map(lambda row: ",".join([str(e) for e in row]))
                | beam.io.WriteToText(
                    file_path_prefix="result_beam",
                    file_name_suffix=".csv",
                    shard_name_template="",
                    header="legal_entity,counterparty,tier,max(rating by counterparty),sum(value where status=ARAP),sum(value where status=ACCR)"
                )
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    s = BeamSolution()
    s.run_pipeline()

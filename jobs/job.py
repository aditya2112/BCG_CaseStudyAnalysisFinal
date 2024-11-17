from pyspark.sql.functions import lit, desc, col, size, array_contains\
, isnan, udf, hour, array_min, array_max, countDistinct
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark
from pyspark.sql import Window
from pyspark.sql import DataFrame
from .settings import settings
from typing import *
from .logger import Logger


class Job(object):
    """Analysis Job Function
    """
    def __init__(self):
        # self.logger = Logger()
        pass
            
    def crashesWithMoreThan2MaleDeaths(self, **kwargs) -> DataFrame:
        """Crashes where more than 2 males were killed."""
        try:
            temp_df = kwargs.get('personDataframe').filter((col('PRSN_GNDR_ID') == 'MALE') & (col('DEATH_CNT') > 0)) \
                .groupBy('CRASH_ID') \
                .agg(F.sum('DEATH_CNT').alias('MALE_DEATH_CNT')) \
                .filter(col('MALE_DEATH_CNT') > 2)
        except Exception as exception:
            print(f"Error: {exception} \nArgs: personDataframe: DataFrame")
        finally:
            return temp_df.count()
    
    def crashesInvolvingTwoWheelers(self, **kwargs) -> DataFrame:
        """Crashes involving two-wheelers."""
        try:
            temp_df = kwargs.get('unitsDataframe').filter(col('VEH_BODY_STYL_ID').rlike('(?i)MOTORCYCLE|SCOOTER')) \
                .select('CRASH_ID').distinct()
        except Exception as exception:
            print(f"Error: {exception} \nArgs: unitsDataframe: DataFrame")
        finally:
            return temp_df.count()
    
    def topVehiclesWithDriverDeathsAndNoAirbags(self, **kwargs) -> DataFrame:
        """Top 5 vehicle makes where the driver died and airbags did not deploy."""
        try:
            temp_df = kwargs.get('unitsDataframe').join(
                kwargs.get('personDataframe').filter((col('DEATH_CNT') > 0) & (col('PRSN_TYPE_ID') == 'DRIVER')),
                ['CRASH_ID', 'UNIT_NBR']
            ).filter(col('PRSN_AIRBAG_ID').rlike('(?i)NOT DEPLOYED')) \
                .groupBy('VEH_MAKE_ID') \
                .count() \
                .orderBy(F.desc('count')) \
                .limit(5)
        except Exception as exception:
            print(f"Error: {exception} \nArgs: unitsDataframe, personDataframe: DataFrames")
        finally:
            return temp_df
    
    def validLicenseDriversInHitAndRun(self, **kwargs) -> DataFrame:
        """Valid license drivers involved in hit-and-run crashes."""
        try:
            temp_df = kwargs.get('personDataframe').join(
                kwargs.get('chargesDataframe').filter(col('CHARGE').rlike('(?i).*HIT AND RUN*')),
                on='CRASH_ID'
            ).filter(col('DRVR_LIC_TYPE_ID') == 'DRIVER LICENSE') \
                .select('CRASH_ID') \
                .distinct()
        except Exception as exception:
            print(f"Error: {exception} \nArgs: personDataframe, chargesDataframe: DataFrames")
        finally:
            return temp_df.count()
    
    def statesWithoutFemalesInAccidents(self, **kwargs) -> DataFrame:
        """Top states with highest crashes involving no females."""
        try:
            temp_df = kwargs.get('personDataframe').filter(col('PRSN_GNDR_ID') != 'FEMALE') \
                .groupBy('CRASH_ID', 'DRVR_LIC_STATE_ID') \
                .count() \
                .orderBy(F.desc('count')) \
                .limit(5)
        except Exception as exception:
            print(f"Error: {exception} \nArgs: personDataframe: DataFrame")
        finally:
            return temp_df
    
    def topVehiclesContributingToInjuries(self, **kwargs) -> DataFrame:
        """Top 3-5 vehicles contributing to injuries."""
        try:
            temp_df = kwargs.get('unitsDataframe').groupBy('VEH_MAKE_ID').agg(F.sum('TOT_INJRY_CNT').alias('InjuryCount')) \
                .orderBy(F.desc('InjuryCount')) \
                .limit(5)
        except Exception as exception:
            print(f"Error: {exception} \nArgs: unitsDataframe: DataFrame")
        finally:
            return temp_df
    
    def topEthnicGroupsForBodyStyles(self, **kwargs) -> DataFrame:
        """Top ethnic group for each body style."""
        try:
            temp_df = kwargs.get('unitsDataframe').join(kwargs.get('personDataframe'), ['CRASH_ID', 'UNIT_NBR']) \
                .groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID') \
                .count() \
                .withColumn("row", F.row_number().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(F.desc("count")))) \
                .filter("row == 1") \
                .drop("row") \
                .orderBy(F.desc("VEH_BODY_STYL_ID"))
        except Exception as exception:
            print(f"Error: {exception} \nArgs: unitsDataframe, personDataframe: DataFrames")
        finally:
            return temp_df
    
    def topZipCodesWithAlcoholCrashes(self, **kwargs) -> DataFrame:
        try:
            temp_df = kwargs.get('personDataframe').filter(
                (col('PRSN_ALC_RSLT_ID').rlike('(?i)positive')) &
                (col('PRSN_ALC_RSLT_ID').isNotNull()) &
                (col('DRVR_ZIP').isNotNull())
            ).groupBy('DRVR_ZIP') \
                .count() \
                .orderBy(F.desc('count')) \
                .limit(5)
        except Exception as exception:
            print(f"Error: {exception} \nArgs: personDataframe: DataFrame")
        finally:
            return temp_df
    
    def crashesWithNoDamagedPropertyAndHighDamageLevel(self, **kwargs) -> DataFrame:

        try:
            merged_df = kwargs.get('unitsDataframe').join(kwargs.get('damageDataframe').select("CRASH_ID", "DAMAGED_PROPERTY"), 
                                                      on="CRASH_ID", how="left")
            no_damaged_property_df = merged_df.filter(col("DAMAGED_PROPERTY").isNull())
            distinct_crash_ids_count = no_damaged_property_df.withColumn('DMAG1_RANGE', F.regexp_extract(col('VEH_DMAG_SCL_1_ID'), "\\d+", 0)) \
                                                        .withColumn('DMAG2_RANGE', F.regexp_extract(col('VEH_DMAG_SCL_2_ID'), "\\d+", 0)) \
                                                        .filter("DMAG1_RANGE > 4 or DMAG2_RANGE > 4") \
                                                        .select('CRASH_ID').distinct().count()
        except Exception as exception:
            print(f"Error: {exception} \nArgs: unitsDataframe, damageDataframe: DataFrames")
        finally:
            return distinct_crash_ids_count
    
    def topSpeedingVehiclesByColorAndState(self, **kwargs) -> DataFrame:
        try:
            top_colors = kwargs.get('unitsDataframe').groupBy("VEH_COLOR_ID") \
                .count().orderBy(F.desc("count")).limit(10) \
                .select("VEH_COLOR_ID").rdd.flatMap(lambda x: x).collect()

            top_states = kwargs.get('unitsDataframe').groupBy("VEH_LIC_STATE_ID") \
                .agg(F.count("CRASH_ID").alias("crash_count")) \
                .orderBy(F.desc("crash_count")).limit(25) \
                .select("VEH_LIC_STATE_ID").rdd.flatMap(lambda x: x).collect()

            merged_df = kwargs.get('unitsDataframe').join(kwargs.get('chargesDataframe'), on="CRASH_ID", how="left")

            filtered_df = merged_df.filter(
            (col("CHARGE").contains("SPEED")) &
            (col("VEH_COLOR_ID").isin(top_colors)) &
            (col("VEH_LIC_STATE_ID").isin(top_states))
            )

            top_vehicle_makes = filtered_df.groupBy("VEH_MAKE_ID").count() \
                .orderBy(F.desc("count")) \
                .limit(5)
        except Exception as exception:
            print(f"Error: {exception} \nArgs: unitsDataframe, chargesDataframe: DataFrames")
        finally:
            return top_vehicle_makes
     
    
    
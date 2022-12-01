
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, BooleanType, DateType, ArrayType

class DatasetSchema:
    schema = StructType() \
      .add("legId",StringType(),True) \
      .add("searchDate",DateType(),True) \
      .add("flightDate",DateType(),True) \
      .add("startingAirport",StringType(),True) \
      .add("destinationAirport",StringType(),True) \
      .add("fareBasisCode",StringType(),True) \
      .add("travelDuration",StringType(),True) \
      .add("elapsedDays",IntegerType(),True) \
      .add("isBasicEconomy",BooleanType(),True) \
      .add("isRefundable",BooleanType(),True) \
      .add("isNonStop",BooleanType(),True) \
      .add("baseFare",DoubleType(),True) \
      .add("totalFare",DoubleType(),True) \
      .add("seatsRemaining",IntegerType(),True) \
      .add("totalTravelDistance",IntegerType(),True) \
      .add("segmentsDepartureTimeEpochSeconds",IntegerType(),True) \
      .add("segmentsDepartureTimeRaw",StringType(),True) \
      .add("segmentsArrivalTimeEpochSeconds",StringType(),True) \
      .add("segmentsArrivalTimeRaw",StringType(),True) \
      .add("segmentsArrivalAirportCode",StringType(),True) \
      .add("segmentsDepartureAirportCode",StringType(),True) \
      .add("segmentsAirlineName",StringType(),True) \
      .add("segmentsAirlineCode",StringType(),True) \
      .add("segmentsEquipmentDescription",StringType(),True) \
      .add("segmentsDurationInSeconds",StringType(),True) \
      .add("segmentsDistance",StringType(),True) \
      .add("segmentsCabinCode",StringType(),True)


# class DatasetSchema:
#     schema = StructType() \
#       .add("legId",StringType(),True) \
#       .add("searchDate",DateType(),True) \
#       .add("flightDate",DateType(),True) \
#       .add("startingAirport",StringType(),True) \
#       .add("destinationAirport",StringType(),True) \
#       .add("fareBasisCode",StringType(),True) \
#       .add("travelDuration",StringType(),True) \
#       .add("elapsedDays",IntegerType(),True) \
#       .add("isBasicEconomy",BooleanType(),True) \
#       .add("isRefundable",BooleanType(),True) \
#       .add("isNonStop",BooleanType(),True) \
#       .add("baseFare",DoubleType(),True) \
#       .add("totalFare",DoubleType(),True) \
#       .add("seatsRemaining",IntegerType(),True) \
#       .add("totalTravelDistance",IntegerType(),True) \
#       .add("segmentsDepartureTimeEpochSeconds",ArrayType(IntegerType()),True) \
#       .add("segmentsDepartureTimeRaw",ArrayType(StringType()),True) \
#       .add("segmentsArrivalTimeEpochSeconds",ArrayType(IntegerType()),True) \
#       .add("segmentsArrivalTimeRaw",ArrayType(StringType()),True) \
#       .add("segmentsArrivalAirportCode",ArrayType(StringType()),True) \
#       .add("segmentsDepartureAirportCode",ArrayType(StringType()),True) \
#       .add("segmentsAirlineName",ArrayType(StringType()),True) \
#       .add("segmentsAirlineCode",ArrayType(StringType()),True) \
#       .add("segmentsEquipmentDescription",ArrayType(StringType()),True) \
#       .add("segmentsDurationInSeconds",ArrayType(IntegerType()),True) \
#       .add("segmentsDistance",ArrayType(IntegerType()),True) \
#       .add("segmentsCabinCode",ArrayType(StringType()),True)

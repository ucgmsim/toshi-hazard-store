from enum import Enum


class AggregationEnum(Enum):
    """Defines the values available for aggregations."""

    MEAN = 'mean'
    COV = 'cov'
    STD = 'std'
    _005 = '0.005'
    _01 = '0.01'
    _025 = '0.025'
    _05 = '0.05'
    _10 = '0.1'
    _20 = '0.2'
    _30 = '0.3'
    _40 = '0.4'
    _50 = '0.5'
    _60 = '0.6'
    _70 = '0.7'
    _80 = '0.8'
    _90 = '0.9'
    _95 = '0.95'
    _975 = '0.975'
    _99 = '0.99'
    _995 = '0.995'


class ProbabilityEnum(Enum):
    """
    Defines the values available for probabilities.

    store values as float representing probability in 1 year
    """

    _86_PCT_IN_50YRS = 3.8559e-02
    _63_PCT_IN_50YRS = 1.9689e-02
    _39_PCT_IN_50YRS = 9.8372e-03
    _18_PCT_IN_50YRS = 3.9612e-03
    _10_PCT_IN_50YRS = 2.1050e-03
    _5_PCT_IN_50YRS = 1.0253e-03
    _2_PCT_IN_50YRS = 4.0397e-04
    _1_PCT_IN_50YRS = 2.0099e-04


class IntensityMeasureTypeEnum(Enum):
    """
    Defines the values available for IMTs.
    """

    PGA = 'PGA'
    SA_0_1 = 'SA(0.1)'
    SA_0_15 = 'SA(0.15)'
    SA_0_2 = 'SA(0.2)'
    SA_0_25 = 'SA(0.25)'
    SA_0_3 = 'SA(0.3)'
    SA_0_35 = 'SA(0.35)'
    SA_0_4 = 'SA(0.4)'
    SA_0_5 = 'SA(0.5)'
    SA_0_6 = 'SA(0.6)'
    SA_0_7 = 'SA(0.7)'
    SA_0_8 = 'SA(0.8)'
    SA_0_9 = 'SA(0.9)'
    SA_1_0 = 'SA(1.0)'
    SA_1_25 = 'SA(1.25)'
    SA_1_5 = 'SA(1.5)'
    SA_1_75 = 'SA(1.75)'
    SA_2_0 = 'SA(2.0)'
    SA_2_5 = 'SA(2.5)'
    SA_3_0 = 'SA(3.0)'
    SA_3_5 = 'SA(3.5)'
    SA_4_0 = 'SA(4.0)'
    SA_4_5 = 'SA(4.5)'
    SA_5_0 = 'SA(5.0)'
    SA_6_0 = 'SA(6.0)'
    SA_7_5 = 'SA(7.5)'
    SA_10_0 = 'SA(10.0)'


class VS30Enum(Enum):
    """
    Defines the values available for VS30.
    """

    _0 = 0  # indicates that this value is not used
    _150 = 150
    _175 = 175
    _200 = 200
    _225 = 225
    _250 = 250
    _275 = 275
    _300 = 300
    _350 = 350
    _375 = 375
    _400 = 400
    _450 = 450
    _500 = 500
    _525 = 525
    _550 = 550
    _600 = 600
    _650 = 650
    _700 = 700
    _750 = 750
    _800 = 800
    _850 = 850
    _900 = 900
    _950 = 950
    _1000 = 1000
    _1050 = 1050
    _1100 = 1100
    _1500 = 1500

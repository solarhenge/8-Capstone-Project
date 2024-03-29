from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [operators.DataQualityOperator
                ,operators.HasRowsOperator
                ,operators.LoadDimensionOperator
                ,operators.LoadFactOperator
                ,operators.LoadTimeOperator
                ,operators.StageToRedshiftOperator
                ]
    helpers = [helpers.SqlQueries
              ]

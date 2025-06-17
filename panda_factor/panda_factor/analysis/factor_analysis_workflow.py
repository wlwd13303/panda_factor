import numpy as np
import pandas as pd
import warnings
import logging
import panda_data
from panda_factor.analysis.factor_func import *
from panda_factor.analysis.factor import factor
from tqdm.auto import tqdm  # Import tqdm for progress bars
from typing import Optional, Any
from panda_common.models.factor_analysis_params import Params
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.config import config
from panda_common.handlers.log_handler import get_factor_logger
import os
from datetime import datetime
import uuid
from loguru import logger


def factor_analysis_workflow(df_factor: pd.DataFrame, adjustment_cycle, group_number, factor_direction) -> None:
    warnings.filterwarnings("ignore")

    # Get task ID from the task
    _db_handler = DatabaseHandler(config)

    # 生成UUID
    task_id = uuid.uuid4().hex
    factor_id = uuid.uuid4().hex
    logger = get_factor_logger(
        task_id=task_id or "unknown",
        factor_id=factor_id or "unknown"
    )
    # start_time=df_factor["date"].unique().tolist().sort()[0]
    # end_time=df_factor["date"].unique().tolist().sort()[-1]
    start_time = sorted(df_factor["date"].unique())[0]
    end_time = sorted(df_factor["date"].unique())[-1]
    # 生成task_id
    # Update status within the thread
    _db_handler.mongo_insert(
        "panda",
        "tasks",
        {
            "task_id": task_id,
            "task_type": "factor_analysis_workflow",
            "process_status": 1,  # Started
            "create_at": datetime.now().isoformat(),
        }
    )
    try:
        # Record analysis start
        logger.info(msg="====== Starting factor analysis ======")

        latest_date = df_factor['date'].max()
        logger.info(msg=f"Latest date: {latest_date}")

        # Initialize data
        panda_data.init()

        # Update status within the thread
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": 2,  # Started
                "updated_at": datetime.now().isoformat(),
            }
        )

        # Get K-line data
        logger.info(msg="1. Starting to fetch K-line data")
        try:
            df_k_data = panda_data.get_market_data(
                start_date=start_time,
                end_date=end_time
            )
            logger.info(msg=f"k-line data length: {len(df_k_data) if df_k_data is not None else 0}")
            print(df_k_data.tail(5) if df_k_data is not None else "K-line data is None")
            logger.info(msg="Cleaning K-line data")
            if df_k_data is not None:
                df_k_data_cleaned = clean_k_data(df_k_data)
                logger.info(msg="Calculating post-adjustment and future returns")
                df_k_data = df_k_data_cleaned.groupby('symbol', group_keys=False).apply(cal_hfq)

        except Exception as e:
            error_msg = f"Failed to fetch K-line data: {str(e)}"
            logger.error(msg=error_msg)
            raise
        logger.info(
            msg=f"K-line data details - rows: {len(df_k_data) if df_k_data is not None else 0}, symbols: {len(df_k_data['symbol'].unique()) if df_k_data is not None else 0}")

        # Update status within the thread
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": 3,  # Started
                "updated_at": datetime.now().isoformat(),
            }
        )
        # Cleaning factor data
        logger.info(msg="2. Starting to clean factor data")
        try:
            factor_list = [df_factor.columns[2]]  # Get the name of the third column and convert to list
            logger.info(msg=f"Factor list: {factor_list}")
            df_factor = df_factor.groupby('date', group_keys=False).apply(
                lambda x: ext_out_3std(x, factor_list[0]))  # 3-sigma extreme value processing
            logger.info(msg="Starting z_score processing")
            df_factor = df_factor.groupby('date', group_keys=False).apply(
                lambda x: z_score(x, factor_list))  # z-score standardization
        except Exception as e:
            error_msg = f"Failed to clean factor data: {str(e)}"
            logger.error(msg=error_msg, extra={"stage": "data_cleaning"})
            raise
        logger.info(
            msg=f"Factor data cleaning details stage: data_cleaning, rows: {len(df_factor) if df_factor is not None else 0}")

        # Update status within the thread
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": 4,  # Started
                "updated_at": datetime.now().isoformat(),
            }
        )

        # Merge data
        logger.info(msg="3. Starting to merge data")
        try:
            df = pd.merge(df_k_data, df_factor, on=['date', 'symbol'], how='left')
            print(len(df))
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
            df = df[df[factor_list].notna().all(axis=1)]
            df = df[df[f'{adjustment_cycle}day_return'].notna()]
        except Exception as e:
            error_msg = f"merge data failed: {str(e)}"
            logger.error(msg=error_msg)
            raise
        print(df.tail(5))
        logger.info(msg=f"Data merge details, rows: {len(df) if df is not None else 0}")

        # Update status within the thread
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": 5,  # Started
                "updated_at": datetime.now().isoformat(),
            }
        )

        # Calculate lagged returns
        logger.info(msg="4. Starting to calculate lagged returns")
        try:
            df = cal_pct_lag(df)
        except Exception as e:
            error_msg = f"Failed to calculate lagged returns: {str(e)}"
            logger.error(msg=error_msg, extra={"stage": "return_calculation"})
            raise

        logger.info(msg="Lagged returns calculation completed")

        # Update status within the thread
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": 6,  # Started
                "updated_at": datetime.now().isoformat(),
            }
        )

        # Factor data grouping
        logger.info(msg=f"5. Starting factor data grouping, group number: {group_number}")
        try:
            # Use group number from parameters
            df_cuted, df_benchmark = grouping_factor(df, factor_list[0], group_number, logger)
        except Exception as e:
            error_msg = f"Factor data grouping failed: {str(e)}"
            logger.error(msg=error_msg, extra={"stage": "grouping"})
            raise

        logger.info(
            msg=f"Factor grouping details, group number: {group_number}, benchmark date count: {len(df_benchmark) if df_benchmark is not None else 0}")

        # Update status within the thread
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": 7,  # Started
                "updated_at": datetime.now().isoformat(),
            }
        )

        def enrich_stock_data(df):
            # Get all unique stock codes
            symbols = df["symbol"].unique().tolist()

            # Query stock names from MongoDB
            query = {'symbol': {'$in': symbols}, 'expired': False}
            cursor = _db_handler.mongo_find(
                "panda",
                "stocks",  # Modify to the correct collection name
                query
            )

            # Create symbol to name mapping dictionary
            symbol_to_name = {item['symbol']: item['name'] for item in cursor}

            # Copy original DataFrame
            result_df = df.copy()

            # Add name column
            result_df['name'] = result_df["symbol"].map(symbol_to_name)

            return result_df

        last_date_top_factor_tmp = df_factor[df_factor['date'] == latest_date].sort_values(by=factor_list[0],
                                                                                           ascending=False).head(20)
        last_date_top_factor_tmp = enrich_stock_data(last_date_top_factor_tmp)
        # Progress bar: In-depth factor analysis
        logger.info(msg="6. Starting in-depth factor analysis")
        factor_obj_list = []
        for f in factor_list:
            try:
                logger.info(msg=f"Analyzing factor {f}")
                # Create factor object with group number
                factor_obj = factor(f, group_number=group_number, factor_id=factor_id)
                # Top 20 factor values for the latest date

                factor_obj.last_date_top_factor = last_date_top_factor_tmp
                factor_obj.logger = logger
                logger.info(msg=f"Retrieved Top20 factor values for latest date {latest_date}")

                factor_obj_list.append(
                    factor_obj)  # Create factor class object to store various backtest parameters and results
                # :param predict_direction: Prediction direction (0 for smaller factor value is better, IC is negative/1 for larger factor value is better, IC is positive)
                factor_obj.set_backtest_parameters(period=adjustment_cycle, predict_direction=factor_direction,
                                                   commission=0)

                logger.info(
                    msg=f"Set backtest parameters: period={adjustment_cycle}, predict_direction={factor_direction}, commission=0")
                logger.info(msg=f"Starting backtest for factor {f}")
                factor_obj.start_backtest(df_cuted, df_benchmark)
                logger.info(msg=f"Completed backtest for factor {f}")
                logger.info(msg=f"7. Saving analysis results for factor {f} to database...")
                # Update status within the thread
                _db_handler.mongo_update(
                    "panda",
                    "tasks",
                    {"task_id": task_id},
                    {
                        "process_status": 8,  # Started
                        "updated_at": datetime.now().isoformat(),
                    }
                )
                factor_obj.inset_to_database(factor_id, task_id)
                logger.info(msg=f"Analysis results for factor {f} saved")
            except Exception as e:
                error_msg = f"Factor {f} analysis failed: {str(e)}"
                logger.error(msg=error_msg, extra={"stage": "factor_analysis"})
                raise

        logger.info(msg="In-depth factor analysis completed", extra={"stage": "factor_analysis"})

        logger.info(msg="======= Factor analysis completed =======")

        # Update status within the thread
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": 9,  # Started
                "updated_at": datetime.now().isoformat(),
            }
        )

    except Exception as e:
        # Record overall error
        error_msg = f"factor analysis failed: {str(e)}"
        logger.error(msg=error_msg, extra={"stage": "error"})
        # Update task status to failed
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": -1,  # Failed
                "error_message": error_msg,
                "updated_at": datetime.now().isoformat(),
            }
        )
        raise  # Re-raise exception
    finally:
        return task_id
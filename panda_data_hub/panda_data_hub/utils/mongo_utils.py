from panda_common.config import config
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger


def ensure_collection_and_indexes(table_name):
    """ 确保集合存在并创建所需的索引 """
    try:
        # 获取数据库对象
        db = DatabaseHandler(config).mongo_client[config["MONGO_DB"]]
        collection_name = table_name
        # 检查集合是否存在
        if collection_name not in db.list_collection_names():
            # 创建集合
            db.create_collection(collection_name)
            logger.info(f"成功创建集合 {collection_name}")
        # 获取集合对象
        collection = db[collection_name]
        # 获取现有的索引信息
        existing_indexes = collection.index_information()
        # 检查是否已存在所需的索引
        if 'symbol_date_idx' not in existing_indexes:
            # 创建复合索引
            collection.create_index(
                [
                    ('symbol', 1),
                    ('date', 1)
                ],
                name='symbol_date_idx',  # 指定索引名称
                background=True  # 后台创建索引，不阻塞其他数据库操作
            )
            logger.info("成功创建索引 symbol_date_idx")
        else:
            logger.info("索引 symbol_date_idx 已存在")

    except Exception as e:
        logger.error(f"创建集合或索引失败: {str(e)}")
        raise  # 抛出异常，因为这是初始化的关键步骤


def ensure_collection_and_indexes_tm(table_name):
    """ 确保集合存在并创建所需的索引 """
    try:
        # 获取数据库对象
        db = DatabaseHandler(config).mongo_client[config["MONGO_DB"]]
        collection_name = table_name
        # 检查集合是否存在
        if collection_name not in db.list_collection_names():
            # 创建集合
            db.create_collection(collection_name)
            logger.info(f"成功创建集合 {collection_name}")
        # 获取集合对象
        collection = db[collection_name]
        # 获取现有的索引信息
        existing_indexes = collection.index_information()
        # 检查是否已存在所需的索引
        if 'symbol_1_date_1' not in existing_indexes:
            # 创建复合索引
            collection.create_index(
                [
                    ('symbol', 1),
                    ('date', 1)
                ],
                name='symbol_1_date_1',  # 指定索引名称
                background=True  # 后台创建索引，不阻塞其他数据库操作
            )
            logger.info("成功创建索引 symbol_1_date_1")
        else:
            logger.info("索引 symbol_1_date_1 已存在")
    except Exception as e:
        logger.error(f"创建集合或索引失败: {str(e)}")
        raise  # 抛出异常，因为这是初始化的关键步骤


def ensure_collection_and_indexes_financial(table_name):
    """
    确保财务数据集合存在并创建所需的索引
    财务数据表使用 (symbol, end_date, ann_date) 复合索引
    """
    try:
        # 获取数据库对象
        db = DatabaseHandler(config).mongo_client[config["MONGO_DB"]]
        collection_name = table_name
        
        # 检查集合是否存在
        if collection_name not in db.list_collection_names():
            # 创建集合
            db.create_collection(collection_name)
            logger.info(f"成功创建财务数据集合 {collection_name}")
        
        # 获取集合对象
        collection = db[collection_name]
        
        # 获取现有的索引信息
        existing_indexes = collection.index_information()
        
        # 创建主要的复合索引：symbol + end_date + ann_date
        index_name = 'symbol_end_date_ann_date_idx'
        if index_name not in existing_indexes:
            collection.create_index(
                [
                    ('symbol', 1),
                    ('end_date', 1),
                    ('ann_date', 1)
                ],
                name=index_name,
                background=True
            )
            logger.info(f"成功创建财务数据索引 {index_name}")
        else:
            logger.info(f"财务数据索引 {index_name} 已存在")
        
        # 创建辅助索引：ann_date（用于按公告日期查询）
        ann_date_index_name = 'ann_date_idx'
        if ann_date_index_name not in existing_indexes:
            collection.create_index(
                [('ann_date', 1)],
                name=ann_date_index_name,
                background=True
            )
            logger.info(f"成功创建财务数据索引 {ann_date_index_name}")
        else:
            logger.info(f"财务数据索引 {ann_date_index_name} 已存在")
        
        # 创建辅助索引：end_date（用于按报告期查询）
        end_date_index_name = 'end_date_idx'
        if end_date_index_name not in existing_indexes:
            collection.create_index(
                [('end_date', 1)],
                name=end_date_index_name,
                background=True
            )
            logger.info(f"成功创建财务数据索引 {end_date_index_name}")
        else:
            logger.info(f"财务数据索引 {end_date_index_name} 已存在")
        
    except Exception as e:
        logger.error(f"创建财务数据集合或索引失败: {str(e)}")
        raise
# factors.py

import pandas as pd
import numpy as np
import re
import ast
from panda_factor.data.data_provider import PandaDataProvider
import time
from panda_common.logger_config import logger
from datetime import datetime
from panda_factor.generate.factor_utils import FactorUtils
from panda_factor.generate.factor_wrapper import FactorDataWrapper, FactorSeries

class MacroFactor:
    """Factor management class, responsible for factor creation and validation"""
    
    # Factor name mapping
    FACTOR_MAP = {
        'price': 'close',
        'volume': 'volume',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'amount': 'amount',
        'returns': 'returns',
        'turnover': 'turnover',
        'market_cap': 'market_cap',
        # Add uppercase version
        'PRICE': 'close',
        'VOLUME': 'volume',
        'OPEN': 'open',
        'HIGH': 'high',
        'LOW': 'low',
        'CLOSE': 'close',
        'AMOUNT': 'amount',
        'RETURNS': 'returns',
        'TURNOVER': 'turnover',
        'MARKET_CAP': 'market_cap',
        # Add mixed case version
        'Price': 'close',
        'Volume': 'volume',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Amount': 'amount',
        'Returns': 'returns',
        'Turnover': 'turnover',
        'Market_Cap': 'market_cap',
    }

    # Allowed built-in functions and modules
    ALLOWED_BUILTINS = {
        # Basic math functions
        'abs', 'round', 'min', 'max', 'sum', 'len',
        'sin', 'cos', 'tan', 'log', 'exp', 'sqrt',
        
        # Basic calculation functions
        'RANK', 'RETURNS', 'STDDEV', 'CORRELATION', 'IF', 'MIN', 'MAX',
        'ABS', 'LOG', 'POWER', 'SIGN', 'SIGNEDPOWER', 'COVARIANCE',
        
        # Time series functions
        'DELAY', 'SUM', 'TS_ARGMAX', 'TS_ARGMIN', 'TS_MEAN', 'TS_MIN',
        'TS_MAX', 'TS_RANK', 'DECAY_LINEAR', 'MA', 'EMA', 'SMA', 'DMA', 'WMA',
        
        # Technical indicator functions
        'MACD', 'KDJ', 'RSI', 'BOLL', 'CCI', 'ATR', 'DMI', 'BBI', 'TAQ',
        'KTN', 'TRIX', 'VR', 'EMV', 'DPO', 'BRAR', 'MTM', 'MASS', 'ROC',
        'EXPMA', 'OBV', 'MFI', 'ASI', 'PSY', 'BIAS', 'WR',
        
        # Price-related functions
        'VWAP', 'CAP', 
        
        # Core utility functions
        'RD', 'RET', 'REF', 'DIFF', 'CONST', 'HHVBARS', 'LLVBARS', 'AVEDEV',
        'SLOPE', 'FORCAST', 'LAST', 'COUNT', 'EVERY', 'EXIST', 'FILTER',
        'SUMIF', 'BARSLAST', 'BARSLASTCOUNT', 'BARSSINCEN', 'CROSS',
        'LONGCROSS', 'VALUEWHEN',
        
        # Average functions
        'MEAN'
    }
    
    # Get all public methods from FactorUtils
    ALLOWED_BUILTINS.update(
        {name for name in dir(FactorUtils) if not name.startswith('_')}
    )

    # Allowed module attributes
    ALLOWED_ATTRIBUTES = {
        'np': {
            # 基础数学运算
            'mean', 'std', 'max', 'min', 'sum', 'abs', 'log', 'exp', 'sqrt',
            'where', 'nan', 'isnan', 'nanmean', 'nansum', 'nanstd',
            # 数组操作
            'array', 'zeros', 'ones', 'full', 'arange', 'linspace',
            'concatenate', 'stack', 'vstack', 'hstack', 'reshape',
            # 统计函数
            'median', 'percentile', 'quantile', 'var', 'cov', 'corrcoef',
            'average', 'cumsum', 'cumprod', 'diff', 'gradient',
            # 条件和比较
            'all', 'any', 'greater', 'greater_equal', 'less', 'less_equal',
            'equal', 'not_equal', 'logical_and', 'logical_or', 'logical_not',
            # 数学函数
            'sin', 'cos', 'tan', 'arcsin', 'arccos', 'arctan', 'sinh', 'cosh',
            'tanh', 'power', 'sign', 'floor', 'ceil', 'round', 'clip',
            # 其他
            'inf', 'pi', 'e', 'newaxis'
        },
        'pd': {
            # 基础类型
            'Series', 'DataFrame', 'Index', 'MultiIndex',
            # 数据检查
            'isna', 'notna', 'isnull', 'notnull',
            # 数据操作
            'concat', 'merge', 'to_datetime', 'date_range', 'DateOffset',
            'Timestamp', 'Timedelta', 'NaT',
            # 分组操作
            'Grouper', 'TimeGrouper',
            # 其他
            'NA', 'NaT', 'read_csv', 'read_excel', 'to_numeric'
        }
    }

    # Explicitly disallowed modules for security
    DISALLOWED_MODULES = {
        'os', 'subprocess', 'sys', 'builtins', 'eval', 'exec', 'globals',
        'locals', 'getattr', 'setattr', 'delattr', '__import__', 'open',
        'compile', 'file', 'execfile', 'shutil', 'pickle', 'shelve',
        'marshal', 'importlib', 'pty', 'platform', 'popen', 'commands'
    }

    # Allowed modules for import
    ALLOWED_IMPORTS = {
        'numpy', 'np',
        'pandas', 'pd',
        'math',
        'datetime', 'timedelta',
        'warnings',
        'Factor',  # 允许从基类导入
        'talib',   # 技术分析库
        'scipy',   # 科学计算
        'sklearn', # 机器学习
        'statsmodels'  # 统计模型
    }

    def __init__(self):
        """Initialize factor calculator"""
        self.data_provider = PandaDataProvider()
        self.base_factors = None  # Used to store base factor data
        
    def _is_safe_name(self, name: str) -> bool:
        """Check if variable name is safe"""
        # If it's a factor name, allow directly
        if name in self.FACTOR_MAP:
            return True
            
        # First check if it's an explicitly disallowed module
        if name in self.DISALLOWED_MODULES:
            print(f"Module access denied: {name}")
            return False
            
        return (name in self.ALLOWED_BUILTINS or 
                name in self.ALLOWED_ATTRIBUTES or
                name in self.ALLOWED_IMPORTS or  # 添加对允许导入模块的检查
                name in {'np', 'pd'} or  # Allow numpy and pandas
                not name.startswith('__'))  # Prevent access to special methods

    def _is_safe_ast(self, node, allow_assign=True, error_info=None) -> bool:
        """Check if AST node is safe
        
        Args:
            node: AST node to check
            allow_assign: Whether to allow assignment operations
            error_info: List to collect error information
        """
        if error_info is None:
            error_info = []
            
        def add_error(node, reason):
            """Helper function to add error information"""
            try:
                line_no = getattr(node, 'lineno', 'unknown')
                col_offset = getattr(node, 'col_offset', 'unknown')
                code_str = ast.unparse(node) if hasattr(ast, 'unparse') else str(node)
                if isinstance(node, (ast.ListComp, ast.GeneratorExp, ast.Lambda)):
                    error_info.append({
                        'line': line_no,
                        'column': col_offset,
                        'type': type(node).__name__,
                        'code': code_str,
                        'reason': reason
                    })
                    return False
            except Exception:
                pass
            return False

        if isinstance(node, ast.Module):
            return all(self._is_safe_ast(subnode, allow_assign, error_info) for subnode in node.body)

        if isinstance(node, (ast.Num, ast.Str, ast.Bytes, ast.NameConstant, ast.Constant)):
            return True

        if isinstance(node, ast.Name):
            if not self._is_safe_name(node.id):
                return add_error(node, f"Unauthorized identifier: {node.id}")
            return True

        if isinstance(node, (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv,
                           ast.Pow, ast.Mod, ast.USub, ast.UAdd)):
            return True

        if isinstance(node, ast.ListComp):
            return add_error(node, "List comprehensions are not allowed")

        if isinstance(node, ast.GeneratorExp):
            return add_error(node, "Generator expressions are not allowed")

        if isinstance(node, ast.Lambda):
            return add_error(node, "Lambda functions are not allowed")

        if isinstance(node, ast.Import):
            # Check imports
            for name in node.names:
                if name.name not in self.ALLOWED_IMPORTS:
                    return add_error(node, f"Unauthorized import: {name.name}")
            return True

        if isinstance(node, ast.ImportFrom):
            # Check from-import statements
            if node.module in self.DISALLOWED_MODULES:
                return add_error(node, f"Import from disallowed module: {node.module}")
            if node.module not in self.ALLOWED_IMPORTS and not any(
                prefix and node.module.startswith(prefix) for prefix in self.ALLOWED_IMPORTS
            ):
                return add_error(node, f"Unauthorized module import: {node.module}")
            return True

        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name):
                module = node.value.id
                if module in self.DISALLOWED_MODULES:
                    return add_error(node, f"Access to disallowed module: {module}")
                if module in ['np', 'pd']:
                    if node.attr in self.ALLOWED_ATTRIBUTES[module]:
                        return True
                    if not node.attr.startswith('_'):
                        return True
                    return add_error(node, f"Access to private {module} attribute: {node.attr}")
                if module in self.ALLOWED_ATTRIBUTES:
                    if node.attr not in self.ALLOWED_ATTRIBUTES[module]:
                        return add_error(node, f"Unauthorized attribute access: {module}.{node.attr}")
                    return True
                return add_error(node, f"Unauthorized module access: {module}.{node.attr}")
            return self._is_safe_ast(node.value, allow_assign=False)

        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                if node.func.id not in self.ALLOWED_BUILTINS:
                    if node.func.id in ['np', 'pd']:
                        return all(self._is_safe_ast(arg, allow_assign=False) for arg in node.args)
                    return add_error(node, f"Unauthorized function call: {node.func.id}")
                return all(self._is_safe_ast(arg, allow_assign=False) for arg in node.args)
            if isinstance(node.func, ast.Attribute):
                if not self._is_safe_ast(node.func, allow_assign=False):
                    return False
                return all(self._is_safe_ast(arg, allow_assign=False) for arg in node.args)
            return add_error(node, "Unsupported call type")

        if isinstance(node, ast.Expr):
            return self._is_safe_ast(node.value, allow_assign=False)
            
        if isinstance(node, ast.BinOp):
            return (self._is_safe_ast(node.left, allow_assign=False) and 
                   isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv,
                                     ast.Pow, ast.Mod)) and
                   self._is_safe_ast(node.right, allow_assign=False))
                   
        if isinstance(node, ast.UnaryOp):
            return (isinstance(node.op, (ast.UAdd, ast.USub)) and 
                   self._is_safe_ast(node.operand, allow_assign=False))
                   
        if isinstance(node, ast.Compare):
            return (all(self._is_safe_ast(comp, allow_assign=False) for comp in node.comparators) and 
                   self._is_safe_ast(node.left, allow_assign=False))

        if isinstance(node, (ast.List, ast.Tuple)):
            return all(self._is_safe_ast(elt, allow_assign=False) for elt in node.elts)
        
        if isinstance(node, ast.Dict):
            return (all(self._is_safe_ast(k, allow_assign=False) for k in node.keys if k is not None) and
                   all(self._is_safe_ast(v, allow_assign=False) for v in node.values))

        if isinstance(node, ast.Assign) and allow_assign:
            return (all(isinstance(target, ast.Name) and self._is_safe_name(target.id) 
                       for target in node.targets) and 
                   self._is_safe_ast(node.value, allow_assign=False))

        if isinstance(node, ast.If):
            return (self._is_safe_ast(node.test, allow_assign=False) and
                   all(self._is_safe_ast(bodynode, allow_assign) for bodynode in node.body) and
                   all(self._is_safe_ast(elsenode, allow_assign) for elsenode in node.orelse))
                   
        if isinstance(node, ast.ClassDef):
            # Check class definition
            if not all(self._is_safe_ast(base) for base in node.bases):
                return add_error(node, f"Unsafe base classes in class {node.name}")
            if node.decorator_list and not all(self._is_safe_ast(dec) for dec in node.decorator_list):
                return add_error(node, f"Unsafe decorators in class {node.name}")
            return all(self._is_safe_ast(stmt) for stmt in node.body)
            
        if isinstance(node, ast.FunctionDef):
            if node.decorator_list and not all(self._is_safe_ast(dec) for dec in node.decorator_list):
                return add_error(node, f"Unsafe decorators in function {node.name}")
            if node.args.defaults and not all(self._is_safe_ast(default) for default in node.args.defaults):
                return add_error(node, f"Unsafe default arguments in function {node.name}")
            return all(self._is_safe_ast(stmt) for stmt in node.body)
            
        if isinstance(node, ast.Subscript):
            if not self._is_safe_ast(node.value, allow_assign=False):
                return False
            if isinstance(node.slice, ast.Constant):
                return True
            elif isinstance(node.slice, ast.Name):
                return self._is_safe_name(node.slice.id)
            elif isinstance(node.slice, ast.Slice):
                return (
                    (node.slice.lower is None or self._is_safe_ast(node.slice.lower, allow_assign=False)) and
                    (node.slice.upper is None or self._is_safe_ast(node.slice.upper, allow_assign=False)) and
                    (node.slice.step is None or self._is_safe_ast(node.slice.step, allow_assign=False))
                )
            else:
                return self._is_safe_ast(node.slice, allow_assign=False)
                
        if isinstance(node, ast.Return):
            if node.value is None:
                return True
            return self._is_safe_ast(node.value, allow_assign=False)

        return True  # 对于其他类型的节点，默认允许

    def _validate_formula(self, formula: str) -> bool:
        """Validate formula safety"""
        try:
            if not isinstance(formula, str):
                print(f"Formula must be string type, got {type(formula)}")
                return False
            
            tree = ast.parse(formula)
            return all(self._is_safe_ast(node) for node in tree.body)
        except SyntaxError as e:
            print(f"Formula syntax error: {e}")
            return False
        except Exception as e:
            print(f"Formula validation error: {e}")
            return False

    def _extract_factor_names(self, formula: str) -> set:
        """Extract required factor names from formula"""
        try:
            if not isinstance(formula, str):
                print(f"Formula must be string type, got {type(formula)}")
                return set()

            pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
            variables = set(re.findall(pattern, formula))
            
            # Remove all built-in functions and attribute names
            variables = variables - self.ALLOWED_BUILTINS - set(self.ALLOWED_ATTRIBUTES.keys())
            
            factor_names = set()
            for var in variables:
                if var.upper() in self.FACTOR_MAP:
                    factor_names.add(self.FACTOR_MAP[var.upper()])
                elif var.lower() in self.FACTOR_MAP:
                    factor_names.add(self.FACTOR_MAP[var.lower()])
                elif var in self.FACTOR_MAP:
                    factor_names.add(self.FACTOR_MAP[var])
            
            if not factor_names:
                print(f"No valid factors found in formula. Variables found: {variables}")
            else:
                print(f"Required factors found: {factor_names}")
            
            return factor_names
        
        except Exception as e:
            print(f"Error extracting factor names: {e}")
            return set()

    def get_base_factors(self, required_factors, start_date: str, end_date: str, symbols: list = None):
        """Get base factor data"""
        if not required_factors:
            return None
        
        factor_data = {}
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import time

        def fetch_factor(factor_name, start_date, end_date, symbols, data_provider):
            try:
                logger.info(f"Starting to get factor {factor_name}...")
                start_time = time.time()
            
                data = data_provider.get_factor_data(factor_name, start_date, end_date, symbols)
                if data is None:
                    logger.error(f"Factor retrieval failed: {factor_name}")
                    return None, factor_name
                    
                # Drop any duplicate date columns before setting index
                data = data.loc[:, ~data.columns.duplicated()]
                data = data.set_index(['date', 'symbol'])
                data = data[~data.index.duplicated(keep='first')]
                    
                logger.info(f"Successfully retrieved factor {factor_name}, took {time.time() - start_time:.2f} seconds")
                return pd.Series(data[factor_name]), factor_name
            except Exception as e:
                logger.error(f"Error retrieving factor {factor_name}: {str(e)}")
                return None, factor_name

        logger.info(f"Starting parallel factor data retrieval, {len(required_factors)} factors in total")
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=min(len(required_factors), 10)) as executor:
            future_to_factor = {
                executor.submit(
                    fetch_factor,
                    factor_name,
                    start_date,
                    end_date, 
                    symbols,
                    self.data_provider
                ): factor_name for factor_name in required_factors
            }

            for future in as_completed(future_to_factor):
                factor_name = future_to_factor[future]
                try:
                    factor_data_result, _ = future.result()
                    if factor_data_result is None:
                        logger.error(f"Factor {factor_name} retrieval failed")
                        return None
                    factor_data[factor_name] = factor_data_result
                    logger.info(f"Factor {factor_name} loaded into memory")
                except Exception as e:
                    logger.error(f"Error processing factor {factor_name}: {str(e)}")
                    return None

        logger.info(f"All factor data retrieval completed, total time taken {time.time() - start_time:.2f} seconds")
        return factor_data

    def create_factor_from_formula(self, factor_logger, formula: str, start_date: str, end_date: str, symbols: list = None):
        """Create factor from formula"""
        print("\n=== Starting formula execution ===")
        print(f"Formula: {formula}")
        
        # Validate formula
        if not isinstance(formula, str):
            raise ValueError("Formula must be string type")
        
        # Extract required factor names
        required_factors = self._extract_factor_names(formula)
        print(f"Required factors found: {required_factors}")
        
        # Get extended start date
        start_date_dt = pd.to_datetime(start_date)
        extended_start_date = (start_date_dt - pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        
        # Create context
        context = {}
        
        # Get base factor data
        self.base_factors = self.get_base_factors(required_factors, extended_start_date, end_date, symbols)
        if self.base_factors is None or any(v is None for v in self.base_factors.values()):
            raise ValueError("Missing required base factors")
        
        # Add base factors to context
        for name, data in self.base_factors.items():
            context[name] = data
            context[name.upper()] = data  # Add uppercase version
            print(f"Adding factor to context: {name} and {name.upper()}")
        
        # Add all FactorUtils methods to context
        for method_name in dir(FactorUtils):
            if not method_name.startswith('_'):  # Skip private methods
                method = getattr(FactorUtils, method_name)
                context[method_name] = method
                context[method_name.upper()] = method  # Add uppercase version
                print(f"Adding function to context: {method_name} and {method_name.upper()}")
        
        # Add math functions to context
        math_funcs = {
            'LOG': np.log,
            'EXP': np.exp,
            'SQRT': np.sqrt,
            'ABS': np.abs,
            'SIN': np.sin,
            'COS': np.cos,
            'TAN': np.tan,
            'POWER': np.power,
            'SIGN': np.sign,
            'MAX': np.maximum,
            'MIN': np.minimum,
            'MEAN': np.mean,
            'STD': np.std
        }
        context.update(math_funcs)
        
        # Add numpy and pandas to context
        context['np'] = np
        context['pd'] = pd
        
        # Prepare result expression
        result_expr = formula.upper()
        print(f"Result expression: {result_expr}")
        
        # Execute formula
        print("Executing result expression")
        try:
            result = eval(result_expr, context)
            print(f"Result type: {type(result)}")
        except Exception as e:
            print(f"Formula execution error: {str(e)}")
            print("Functions available in context:")
            for key in sorted(context.keys()):
                if callable(context[key]):
                    print(f"- {key}")
            raise
        
        # Ensure result is pandas Series
        if not isinstance(result, pd.Series):
            print("Converting result to pandas Series")
            result = pd.Series(result)
        
        # Ensure result has correct index
        if not isinstance(result.index, pd.MultiIndex):
            print("Converting result index to MultiIndex")
            result = pd.Series(result, index=pd.MultiIndex.from_tuples(
                [(d, s) for d, s in zip(result.index, result.index)],
                names=['date', 'symbol']
            ))
        
        # Ensure index names are correct
        if result.index.names != ['date', 'symbol']:
            print("Setting correct index names")
            result.index.names = ['date', 'symbol']
        
        # Ensure index is unique
        result = result[~result.index.duplicated(keep='first')]
        
        print("Formula execution completed")
        return result.to_frame(name='value')

    def _format_error_stack(self, error: Exception, code: str) -> str:
        """Format error stack information, focusing on user code error location
        
        Args:
            error: Exception caught
            code: User's original code
            
        Returns:
            Formatted error information
        """
        import traceback
        import re
        
        # Split user code into lines for easier subsequent display
        code_lines = [line.rstrip() for line in code.split('\n')]
        
        # Build error information
        error_msg = [f"Error type: {type(error).__name__}"]
        error_msg.append(f"Error message: {str(error)}")
        
        # Try to extract line number from error message
        line_no = None
        
        # Get full stack trace information
        tb_list = traceback.extract_tb(error.__traceback__)
        
        # Find user code error location
        for frame in reversed(tb_list):  # Search from last frame
            # Check if it's user code frame
            if frame.filename in ['<string>', '<unknown>']:
                # Get actual code line content
                code_line = frame.line if frame.line else ""
                if not code_line.strip():
                    continue
                    
                # Search for this line in code
                for i, line in enumerate(code_lines, 1):
                    if line.strip() and code_line.strip() in line.strip():
                        line_no = i
                        break
                if line_no:  # If line number found, stop searching
                    break
        
        # If stack trace doesn't find line number, try to extract from error message
        if line_no is None:
            error_str = str(error)
            # Try to match "line X" pattern
            line_match = re.search(r'line (\d+)', error_str)
            if line_match:
                line_no = int(line_match.group(1))
                
            # If NameError, try to find undefined variable name in code
            if isinstance(error, NameError):
                var_match = re.search(r"name '(.+)' is not defined", str(error))
                if var_match:
                    var_name = var_match.group(1)
                    for i, line in enumerate(code_lines, 1):
                        if var_name in line:
                            line_no = i
                            break
        
        if line_no is not None:
            # Adjust line number to match user code
            # Skip leading empty lines
            while line_no > 1 and not code_lines[line_no - 1].strip():
                line_no -= 1
                
            error_msg.append("\nCode location:")
            
            # Display code around error line (context)
            start_line = max(0, line_no - 2)  # Display 2 lines before error line
            end_line = min(len(code_lines), line_no + 3)  # Display 2 lines after error line
            
            for i in range(start_line, end_line):
                if i < len(code_lines):
                    # Use >>> to mark error line, indent other lines
                    prefix = '>>> ' if i + 1 == line_no else '    '
                    # Display line number and code content
                    error_msg.append(f"{prefix}{i + 1:2d} | {code_lines[i]}")
        else:
            # If no specific line number found, display related code context
            error_msg.append("\nUnable to determine specific error line, displaying related code:")
            for i, line in enumerate(code_lines, 1):
                if line.strip():  # Display only non-empty lines
                    error_msg.append(f"    {i:2d} | {line}")
        
        return '\n'.join(error_msg)

    def create_factor_from_class(self, factor_logger, class_code: str, start_date: str, end_date: str, symbols: list = None):
        """Create factor from class"""
        from .factor_loader import FactorLoader
        import traceback
        
        # Parse code and check safety
        try:
            tree = ast.parse(class_code)
            unsafe_operations = []
            
            # Check all top-level nodes
            for node in tree.body:
                if not self._is_safe_ast(node):
                    if isinstance(node, ast.Import):
                        unsafe_operations.append(f"Unsafe import: {', '.join(name.name for name in node.names)}")
                    elif isinstance(node, ast.ImportFrom):
                        unsafe_operations.append(f"Unsafe import: {node.module}")
                    elif isinstance(node, ast.ClassDef):
                        unsafe_operations.append(f"Unsafe operation in class {node.name}")
                    else:
                        unsafe_operations.append(f"Unsafe operation: {type(node).__name__}")
            
            # Additional check for imports and system calls
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for name in node.names:
                        if name.name in self.DISALLOWED_MODULES:
                            unsafe_operations.append(f"Found disallowed import: {name.name}")
                elif isinstance(node, ast.ImportFrom):
                    if node.module in self.DISALLOWED_MODULES:
                        unsafe_operations.append(f"Found disallowed import: {node.module}")
                # Check dangerous system calls
                elif isinstance(node, ast.Call):
                    if isinstance(node.func, ast.Attribute):
                        if node.func.attr in ['system', 'popen', 'exec', 'eval', 'execfile', 'compile']:
                            unsafe_operations.append(f"Found dangerous system call: {node.func.attr}")
            
            if unsafe_operations:
                for op in unsafe_operations:
                    factor_logger.warning(op)
                factor_logger.error("Code safety check failed, contains unsafe operations")
                return None
                
            # factor_logger.info("Code safety check passed")
        except SyntaxError as e:
            factor_logger.error(f"Code syntax error: {e}")
            factor_logger.error(f"Error location: Line {e.lineno}, Column {e.offset}")
            factor_logger.error(f"Error code: {e.text}")
            factor_logger.error(f"Error stack:\n{self._format_error_stack(e, class_code)}")
            return None
        except Exception as e:
            factor_logger.error(f"Code parsing error: {str(e)}")
            factor_logger.error(f"Error stack:\n{self._format_error_stack(e, class_code)}")
            return None
        
        # Load factor class
        try:
            factor_class = FactorLoader.load_factor_class(class_code, common_imports="""
import numpy as np
import pandas as pd
# import talib
# from scipy import stats
# from sklearn import preprocessing
import math
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')
""")
            if factor_class is None:
                factor_logger.error("Factor class load failed")
                return None
                
            # Create factor instance
            factor = factor_class()
            factor.set_factor_logger(factor_logger)
            
            # Add custom print function to instance namespace
            def custom_print(*args, **kwargs):
                current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{current_date}]", *args, **kwargs)
            factor.print = custom_print
            
        except Exception as e:
            factor_logger.error(f"Factor class initialization failed: {str(e)}")
            factor_logger.error(f"Error stack:\n{self._format_error_stack(e, class_code)}")
            return None
        
        try:
            # Extract required factors
            required_factors = set()
            
            # Find all dictionary subscript operations
            for node in ast.walk(tree):
                if (isinstance(node, ast.Subscript) and 
                    isinstance(node.value, ast.Name) and 
                    node.value.id == 'factors' and
                    isinstance(node.slice, ast.Constant)):
                    required_factors.add(node.slice.value)
            
            if not required_factors:
                factor_logger.error("No factor requirements found in code")
                return None
            
            # Get extended start date
            start_date_dt = pd.to_datetime(start_date)
            extended_start_date = (start_date_dt - pd.DateOffset(months=3)).strftime('%Y-%m-%d')
            
            # Get required factors
            factors = {}
            from concurrent.futures import ThreadPoolExecutor, as_completed

            def fetch_factor(factor_name, extended_start_date, end_date, symbols, data_provider):
                try:
                    data = data_provider.get_factor_data(factor_name, extended_start_date, end_date, symbols)
                    if data is None:
                        factor_logger.error(f"Factor retrieval failed: {factor_name}")
                        return None, factor_name
                    # Remove duplicate columns
                    data = data.loc[:, ~data.columns.duplicated()]
                    data = data.set_index(['date', 'symbol'])
                    data = data[~data.index.duplicated(keep='first')]
                    data = data.sort_index(level='date')
                    return data[factor_name], factor_name
                except Exception as e:
                    factor_logger.error(f"Error retrieving factor {factor_name}: {str(e)}")
                    factor_logger.error(f"Error stack:\n{self._format_error_stack(e, class_code)}")
                    return None, factor_name

            with ThreadPoolExecutor(max_workers=min(len(required_factors), 10)) as executor:
                future_to_factor = {
                    executor.submit(
                        fetch_factor, 
                        factor_name, 
                        extended_start_date, 
                        end_date, 
                        symbols,
                        self.data_provider
                    ): factor_name for factor_name in required_factors
                }

                for future in as_completed(future_to_factor):
                    factor_data, factor_name = future.result()
                    if factor_data is None:
                        return None
                    factors[factor_name] = factor_data
            
            # Use wrapper class to wrap factor data
            wrapped_factors = FactorDataWrapper(factors)
            
            # Calculate factor value
            try:
                result = factor.calculate(wrapped_factors)
            except Exception as e:
                factor_logger.error(f"Factor calculation failed: {str(e)}")
                factor_logger.error(f"Error stack:\n{self._format_error_stack(e, class_code)}")
                return None
            
            # Verify result
            if not isinstance(result, pd.Series):
                factor_logger.info("Converting result to pandas Series")
                if isinstance(result, FactorSeries):
                    # If FactorSeries, directly get its series attribute
                    result = result.series
                elif isinstance(result, pd.DataFrame):
                    # If DataFrame, convert to Series
                    result = result.iloc[:,0]
                else:
                    # Other types, try to convert to Series and set correct index
                    try:
                        result = pd.Series(result)
                        result.index = pd.MultiIndex.from_tuples(
                            [(d, s) for d, s in zip(result.index, result.index)],
                            names=['date', 'symbol']
                        )
                    except Exception as e:
                        factor_logger.error(f"Result conversion failed: {str(e)}")
                        factor_logger.error(f"Error stack:\n{self._format_error_stack(e, class_code)}")
                        return None
                
            # Ensure result is sorted by date
            result = result.sort_index(level='date')
            
            # Filter out dates before start_date
            result = result[result.index.get_level_values('date') >= start_date]
            
            # Ensure result is Series type
            if not isinstance(result, pd.Series):
                result = pd.Series(result)
            
            # Ensure result has correct index
            if not isinstance(result.index, pd.MultiIndex):
                factor_logger.info("Converting result index to MultiIndex")
                result = pd.Series(result, index=pd.MultiIndex.from_tuples(
                    [(d, s) for d, s in zip(result.index, result.index)],
                    names=['date', 'symbol']
                ))
            elif result.index.nlevels == 2 and result.index.names != ['date', 'symbol']:
                factor_logger.info("Setting correct index names")
                result.index.names = ['date', 'symbol']
            elif result.index.nlevels == 3:
                factor_logger.info("Fixing three-level index issue")
                # Get unique date and stock codes
                dates = result.index.get_level_values(1).unique()
                symbols = result.index.get_level_values(2).unique()
                
                # Create new secondary index
                new_index = pd.MultiIndex.from_product(
                    [dates, symbols],
                    names=['date', 'symbol']
                )
                
                # Reindex data
                result = result.reset_index(level=0, drop=True)  # Delete first level index
                result = result.reindex(new_index)
            
            # Ensure index is unique
            result = result[~result.index.duplicated(keep='first')]
            
            return result.to_frame(name='value')
            
        except Exception as e:
            factor_logger.error(f"Error occurred during factor processing: {str(e)}")
            factor_logger.error(f"Error stack:\n{self._format_error_stack(e, class_code)}")
            return None
    
    def validate_factor(self, code: str, code_type: str = 'formula', timeout: int = 5) -> dict:
        """Validate factor code"""
        result = {
            'is_valid': True,
            'syntax_errors': [],
            'missing_factors': [],
            'formula_errors': [],
            'unsafe_operations': [],
            'timeout': False
        }
        
        try:
            # Parse code
            try:
                tree = ast.parse(code)
            except SyntaxError as e:
                result['is_valid'] = False
                result['syntax_errors'].append(f"Syntax error at line {e.lineno}: {e.msg}")
                return result
            
            # Check for unsafe operations
            error_info = []
            for node in ast.walk(tree):
                self._is_safe_ast(node, error_info=error_info)
            
            # Only collect important unsafe operations
            if error_info:
                result['is_valid'] = False
                for err in error_info:
                    result['unsafe_operations'].append(
                        f"Line {err['line']}: {err['type']} is not allowed - {err['reason']}"
                    )
                return result
            
            # Continue with other validations...
            
        except Exception as e:
            result['is_valid'] = False
            result['syntax_errors'].append(f"Validation error: {str(e)}")
            
        return result
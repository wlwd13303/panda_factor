"""
财务数据字段配置 - 集中管理

这个模块定义了所有财务报表的字段列表，被以下模块使用：
1. panda_data_hub - tushare数据采集时指定需要获取的字段
2. panda_data - factor_reader 判断哪些字段是财务字段，需要前向填充
3. panda_factor - 因子计算时识别财务因子
"""

# ============================================================================
# 基础字段（所有财务报表都有的字段，不参与因子计算）
# ============================================================================

BASE_FIELDS = [
    'ts_code',      # tushare股票代码
    'symbol',       # 标准股票代码（我们的格式）
    'end_date',     # 报告期
    'ann_date',     # 公告日期
    'f_ann_date',   # 实际公告日期
    'report_type',  # 报告类型（1-合并报表 2-单季度）
    'update_flag',  # 更新标志（0-首次 1-修正）
    'comp_type',    # 公司类型（1-一般工商业 2-银行 3-保险 4-证券）
]


# ============================================================================
# 利润表字段（financial_income）
# ============================================================================

INCOME_FIELDS = [
    # 收入类
    'total_revenue',        # 营业总收入
    'revenue',             # 营业收入
    'int_income',          # 利息收入（金融）
    'prem_earned',         # 已赚保费（保险）
    'comm_income',         # 手续费及佣金收入（金融）
    'n_commis_income',     # 手续费及佣金净收入（金融）
    'n_oth_income',        # 其他经营净收益
    'n_oth_b_income',      # 其他业务收入
    'prem_income',         # 保险业务收入（保险）
    'out_prem',            # 分出保费（保险）
    'une_prem_reser',      # 提取未到期责任准备金（保险）
    'reins_income',        # 分保费收入（保险）
    'n_sec_tb_income',     # 代理买卖证券业务净收入（证券）
    'n_sec_uw_income',     # 证券承销业务净收入（证券）
    'n_asset_mg_income',   # 受托客户资产管理业务净收入（金融）
    
    # 成本类
    'oper_cost',           # 营业总成本
    'int_exp',             # 利息支出（金融）
    'comm_exp',            # 手续费及佣金支出（金融）
    'biz_tax_surchg',      # 营业税金及附加
    'sell_exp',            # 销售费用
    'admin_exp',           # 管理费用
    'fin_exp',             # 财务费用
    'assets_impair_loss',  # 资产减值损失
    'prem_refund',         # 退保金（保险）
    'compens_payout',      # 赔付支出（保险）
    'reser_insur_liab',    # 提取保险责任准备金（保险）
    'div_payt',            # 保单红利支出（保险）
    'reins_exp',           # 分保费用（保险）
    'oper_exp',            # 业务及管理费（金融）
    'compens_payout_refu', # 减：摊回赔付支出（保险）
    'insur_reser_refu',    # 减：摊回保险责任准备金（保险）
    'reins_cost_refund',   # 减：摊回分保费用（保险）
    'other_bus_cost',      # 其他业务成本
    
    # 利润类
    'operate_profit',      # 营业利润
    'non_oper_income',     # 营业外收入
    'non_oper_exp',        # 营业外支出
    'nca_disploss',        # 非流动资产处置损失
    'total_profit',        # 利润总额
    'income_tax',          # 所得税费用
    'n_income',            # 净利润（含少数股东损益）
    'n_income_attr_p',     # 归属于母公司所有者的净利润
    'minority_gain',       # 少数股东损益
    'oth_compr_income',    # 其他综合收益
    't_compr_income',      # 综合收益总额
    'compr_inc_attr_p',    # 归属于母公司所有者的综合收益总额
    'compr_inc_attr_m_s',  # 归属于少数股东的综合收益总额
    
    # 每股指标
    'ebit',                # 息税前利润
    'ebitda',              # 息税折旧摊销前利润
    'insurance_exp',       # 保险业务支出（保险）
    'undist_profit',       # 年初未分配利润
    'distable_profit',     # 可分配利润
    'rd_exp',              # 研发费用
    
    # 单季度指标（如果是单季度报表）
    'q_revenue',           # 单季度营业收入
    'q_oper_cost',         # 单季度营业成本
    'q_operate_profit',    # 单季度营业利润
    'q_total_profit',      # 单季度利润总额
    'q_n_income',          # 单季度净利润
    'q_n_income_attr_p',   # 单季度归母净利润
]


# ============================================================================
# 资产负债表字段（financial_balance）
# ============================================================================

BALANCE_FIELDS = [
    # 资产类 - 流动资产
    'total_assets',              # 资产总计
    'total_cur_assets',          # 流动资产合计
    'money_cap',                 # 货币资金
    'trad_asset',                # 交易性金融资产
    'notes_receiv',              # 应收票据
    'accounts_receiv',           # 应收账款
    'oth_receiv',                # 其他应收款
    'prepayment',                # 预付款项
    'div_receiv',                # 应收股利
    'int_receiv',                # 应收利息
    'inventories',               # 存货
    'amor_exp',                  # 待摊费用
    'nca_within_1y',             # 一年内到期的非流动资产
    'sett_rsrv',                 # 结算备付金（金融）
    'loanto_oth_bank_fi',        # 拆出资金（金融）
    'premium_receiv',            # 应收保费（保险）
    'reinsur_receiv',            # 应收分保账款（保险）
    'reinsur_res_receiv',        # 应收分保合同准备金（保险）
    'pur_resale_fa',             # 买入返售金融资产（金融）
    'oth_cur_assets',            # 其他流动资产
    
    # 资产类 - 非流动资产
    'total_nca',                 # 非流动资产合计
    'fa_avail_for_sale',         # 可供出售金融资产
    'htm_invest',                # 持有至到期投资
    'lt_eqt_invest',             # 长期股权投资
    'invest_real_estate',        # 投资性房地产
    'time_deposits',             # 定期存款
    'oth_assets',                # 其他资产
    'lt_rec',                    # 长期应收款
    'fix_assets',                # 固定资产
    'cip',                       # 在建工程
    'const_materials',           # 工程物资
    'fixed_assets_disp',         # 固定资产清理
    'produc_bio_assets',         # 生产性生物资产
    'oil_and_gas_assets',        # 油气资产
    'intan_assets',              # 无形资产
    'r_and_d',                   # 研发支出
    'goodwill',                  # 商誉
    'lt_amor_exp',               # 长期待摊费用
    'defer_tax_assets',          # 递延所得税资产
    'decr_in_disbur',            # 发放贷款及垫款（金融）
    'oth_nca',                   # 其他非流动资产
    
    # 负债类 - 流动负债
    'total_liab',                # 负债合计
    'total_cur_liab',            # 流动负债合计
    'st_borr',                   # 短期借款
    'borrow_oth_bank_fi',        # 向中央银行借款（金融）
    'trading_fl',                # 交易性金融负债
    'notes_payable',             # 应付票据
    'acct_payable',              # 应付账款
    'adv_receipts',              # 预收款项
    'sold_for_repur_fa',         # 卖出回购金融资产款（金融）
    'comm_payable',              # 应付手续费及佣金（金融）
    'payroll_payable',           # 应付职工薪酬
    'taxes_payable',             # 应交税费
    'int_payable',               # 应付利息
    'div_payable',               # 应付股利
    'oth_payable',               # 其他应付款
    'acc_exp',                   # 预提费用
    'deferred_inc',              # 递延收益
    'st_bonds_payable',          # 应付短期债券
    'payable_to_reinsurer',      # 应付分保账款（保险）
    'rsrv_insur_cont',           # 保险合同准备金（保险）
    'acting_trading_sec',        # 代理买卖证券款（证券）
    'acting_uw_sec',             # 代理承销证券款（证券）
    'non_cur_liab_due_1y',       # 一年内到期的非流动负债
    'oth_cur_liab',              # 其他流动负债
    
    # 负债类 - 非流动负债
    'total_ncl',                 # 非流动负债合计
    'lt_borr',                   # 长期借款
    'bonds_payable',             # 应付债券
    'lt_payable',                # 长期应付款
    'specific_payables',         # 专项应付款
    'estimated_liab',            # 预计负债
    'defer_tax_liab',            # 递延所得税负债
    'defer_inc_non_cur_liab',    # 递延收益-非流动负债
    'oth_ncl',                   # 其他非流动负债
    
    # 所有者权益类
    'total_hldr_eqy_exc_min_int', # 股东权益合计（不含少数股东权益）
    'total_hldr_eqy_inc_min_int', # 股东权益合计（含少数股东权益）
    'cap_rese',                   # 资本公积金
    'prov_nom_risks',             # 风险准备金（金融）
    'general_risk_reserve',       # 一般风险准备（金融）
    'unassign_rpofit',            # 未分配利润
    'retained_profit',            # 留存收益
    'tsy_stk',                    # 库存股
    'special_rese',               # 专项储备
    'surplus_rese',               # 盈余公积金
    'ordin_risk_reser',           # 一般风险准备金
    'equity_trans_adjust',        # 其他权益工具
    'oth_eqt_tools',              # 其他权益工具
    'oth_eqt_tools_p_shr',        # 其他权益工具（优先股）
    'minority_int',               # 少数股东权益
    'total_equity',               # 所有者权益合计
    'total_liab_hldr_eqy',        # 负债及股东权益总计
    'lt_payroll_payable',         # 长期应付职工薪酬
    'oth_comp_income',            # 其他综合收益
    'oth_eqt_tools_c_shr',        # 其他权益工具（永续债）
    'settle_assets',              # 结算备付金
    'lend_fund',                  # 拆出资金
    'payable_refund_deposits',    # 应付赔付款
]


# ============================================================================
# 现金流量表字段（financial_cashflow）
# ============================================================================

CASHFLOW_FIELDS = [
    # 经营活动现金流
    'c_fr_sale_sg',               # 销售商品、提供劳务收到的现金
    'recp_tax_rends',             # 收到的税费返还
    'n_depos_incr_fi',            # 客户存款和同业存放款项净增加额（金融）
    'n_incr_loans_cb',            # 向中央银行借款净增加额（金融）
    'n_inc_borr_oth_fi',          # 向其他金融机构拆入资金净增加额（金融）
    'prem_fr_orig_contr',         # 收到原保险合同保费取得的现金（保险）
    'n_incr_insured_dep',         # 保户储金净增加额（保险）
    'n_reinsur_prem',             # 收到再保业务现金净额（保险）
    'n_incr_disp_tfa',            # 处置交易性金融资产净增加额
    'ifc_cash_incr',              # 收取利息和手续费净增加额（金融）
    'n_incr_disp_faas',           # 处置可供出售金融资产净增加额
    'n_incr_loans_oth_bank',      # 拆入资金净增加额（金融）
    'n_cap_incr_repur',           # 回购业务资金净增加额（金融）
    'c_fr_oth_operate_a',         # 收到其他与经营活动有关的现金
    'c_inf_fr_operate_a',         # 经营活动现金流入小计
    'c_paid_goods_s',             # 购买商品、接受劳务支付的现金
    'c_paid_to_for_empl',         # 支付给职工以及为职工支付的现金
    'c_paid_for_taxes',           # 支付的各项税费
    'n_incr_clt_loan_adv',        # 客户贷款及垫款净增加额（金融）
    'n_incr_dep_cbob',            # 存放央行和同业款项净增加额（金融）
    'c_pay_claims_orig_inco',     # 支付原保险合同赔付款项的现金（保险）
    'pay_handling_chrg',          # 支付手续费的现金（金融）
    'pay_comm_insur_plcy',        # 支付保单红利的现金（保险）
    'c_paid_oth_operate_a',       # 支付其他与经营活动有关的现金
    'c_outf_fr_operate_a',        # 经营活动现金流出小计
    'n_cashflow_act',             # 经营活动产生的现金流量净额
    
    # 投资活动现金流
    'c_disp_withdrwl_invest',     # 收回投资收到的现金
    'c_recp_return_invest',       # 取得投资收益收到的现金
    'n_recp_disp_fiolta',         # 处置固定资产、无形资产和其他长期资产收回的现金净额
    'n_recp_disp_sobu',           # 处置子公司及其他营业单位收到的现金净额
    'stot_inflows_inv_act',       # 投资活动现金流入小计
    'c_pay_acq_const_fiolta',     # 购建固定资产、无形资产和其他长期资产支付的现金
    'c_paid_invest',              # 投资支付的现金
    'n_disp_subs_oth_biz',        # 取得子公司及其他营业单位支付的现金净额
    'c_paid_oth_invest_a',        # 支付其他与投资活动有关的现金
    'stot_out_inv_act',           # 投资活动现金流出小计
    'n_cashflow_inv_act',         # 投资活动产生的现金流量净额
    
    # 筹资活动现金流
    'c_recp_cap_contrib',         # 吸收投资收到的现金
    'incl_cash_rec_saims',        # 其中：子公司吸收少数股东投资收到的现金
    'c_recp_borro',               # 取得借款收到的现金
    'proc_issue_bonds',           # 发行债券收到的现金
    'c_recp_oth_fnc_a',           # 收到其他与筹资活动有关的现金
    'stot_cash_in_fnc_act',       # 筹资活动现金流入小计
    'c_prepay_amt_borr',          # 偿还债务支付的现金
    'c_pay_dist_dpcp_int_exp',    # 分配股利、利润或偿付利息支付的现金
    'incl_dvd_profit_paid_sc_ms', # 其中：子公司支付给少数股东的股利、利润
    'c_paid_oth_fnc_a',           # 支付其他与筹资活动有关的现金
    'stot_cashout_fnc_act',       # 筹资活动现金流出小计
    'n_cashflow_fnc_act',         # 筹资活动产生的现金流量净额
    
    # 汇率变动及现金净增加
    'eff_fx_flu_cash',            # 汇率变动对现金的影响
    'n_incr_cash_cash_equ',       # 现金及现金等价物净增加额
    'c_cash_equ_beg_period',      # 期初现金及现金等价物余额
    'c_cash_equ_end_period',      # 期末现金及现金等价物余额
    
    # 补充项目
    'c_recp_cap_contrib_sup',     # 吸收投资收到的现金（补充）
    'c_pay_dist_dpcp_sup',        # 分配股利支付的现金（补充）
    'uncon_invest_loss',          # 未确认投资损失
    'prov_depr_assets',           # 资产减值准备
    'depr_fa_coga_dpba',          # 固定资产折旧、油气资产折耗、生产性生物资产折旧
    'amort_intang_assets',        # 无形资产摊销
    'lt_amort_deferred_exp',      # 长期待摊费用摊销
    'decr_deferred_exp',          # 待摊费用的减少
    'incr_acc_exp',               # 预提费用的增加
    'loss_disp_fiolta',           # 处置固定无形资产和其他长期资产的损失
    'loss_scr_fa',                # 固定资产报废损失
    'loss_fv_chg',                # 公允价值变动损失
    'invest_loss',                # 投资损失
    'decr_def_inc_tax_assets',    # 递延所得税资产减少
    'incr_def_inc_tax_liab',      # 递延所得税负债增加
    'decr_inventories',           # 存货的减少
    'decr_oper_payable',          # 经营性应收项目的减少
    'incr_oper_payable',          # 经营性应付项目的增加
    'others',                     # 其他
    'im_net_cashflow_oper_act',   # 经营活动产生的现金流量净额（间接法）
    'conv_debt_into_cap',         # 债务转为资本
    'conv_copbonds_due_within_1y', # 一年内到期的可转换公司债券
    'fa_fnc_leases',              # 融资租入固定资产
    'im_n_incr_cash_equ',         # 现金及现金等价物净增加额（间接法）
    'net_dism_capital_add',       # 拆出资金净增加额
    'net_cash_rece_sec',          # 代理买卖证券收到的现金净额
    'credit_impa_loss',           # 信用减值损失
    'use_right_asset_dep',        # 使用权资产折旧
    'oth_loss_asset',             # 其他资产减值损失
    'end_bal_cash',               # 现金的期末余额
    'beg_bal_cash',               # 现金的期初余额
    'end_bal_cash_equ',           # 现金等价物的期末余额
    'beg_bal_cash_equ',           # 现金等价物的期初余额
]


# ============================================================================
# 财务指标字段（financial_indicator）
# ============================================================================

INDICATOR_FIELDS = [
    # 每股指标
    'eps',                  # 基本每股收益
    'dt_eps',               # 稀释每股收益
    'total_revenue_ps',     # 每股营业总收入
    'revenue_ps',           # 每股营业收入
    'capital_rese_ps',      # 每股资本公积
    'surplus_rese_ps',      # 每股盈余公积
    'undist_profit_ps',     # 每股未分配利润
    'extra_item',           # 非经常性损益
    'profit_dedt',          # 扣除非经常性损益后的净利润
    
    # 成长能力指标
    'q_gr_revenue',         # 单季度营业收入增长率(%)
    'q_gr_profit',          # 单季度营业利润增长率(%)
    'q_revenue_yoy',        # 单季度营业收入同比增长率(%)
    'q_profit_yoy',         # 单季度营业利润同比增长率(%)
    'q_netprofit_yoy',      # 单季度归母净利润同比增长率(%)
    
    # 盈利能力指标
    'roe',                  # 净资产收益率ROE
    'roe_waa',              # 加权平均净资产收益率
    'roe_dt',               # 净资产收益率(扣除非经常损益)
    'roa',                  # 总资产报酬率ROA
    'npta',                 # 总资产净利润
    'roic',                 # 投入资本回报率ROIC
    'roe_yearly',           # 年化净资产收益率
    'roa2_yearly',          # 年化总资产报酬率
    'roe_avg',              # 平均净资产收益率(增发条件)
    'opincome_of_ebt',      # 经营活动净收益/利润总额
    'investincome_of_ebt',  # 价值变动净收益/利润总额
    'n_op_profit_of_ebt',   # 营业外收支净额/利润总额
    'tax_to_ebt',           # 所得税/利润总额
    
    # 营运能力指标
    'ar_turn',              # 应收账款周转率
    'ar_days',              # 应收账款周转天数
    'inv_turn',             # 存货周转率
    'inv_days',             # 存货周转天数
    'current_turn',         # 流动资产周转率
    'current_days',         # 流动资产周转天数
    'ca_turn',              # 流动资产周转率
    'fa_turn',              # 固定资产周转率
    'assets_turn',          # 总资产周转率
    
    # 财务风险指标
    'debt_to_assets',       # 资产负债率
    'assets_to_eqt',        # 权益乘数
    'dp_assets_to_eqt',     # 权益乘数(杜邦分析)
    'ca_to_assets',         # 流动资产/总资产
    'nca_to_assets',        # 非流动资产/总资产
    'tbassets_to_totalassets', # 有形资产/总资产
    'int_to_talcap',        # 带息债务/全部投入资本
    'eqt_to_talcapital',    # 归属于母公司的股东权益/全部投入资本
    'currentdebt_to_debt',  # 流动负债/负债合计
    'longdeb_to_debt',      # 非流动负债/负债合计
    
    # 偿债能力指标
    'current_ratio',        # 流动比率
    'quick_ratio',          # 速动比率
    'cash_ratio',           # 保守速动比率
    'salecash_to_or',       # 销售商品提供劳务收到的现金/营业收入
    'ocf_to_or',            # 经营活动产生的现金流量净额/营业收入
    'ocf_to_opincome',      # 经营活动产生的现金流量净额/经营活动净收益
    'capitalized_to_da',    # 资本化比率
    
    # 利润率指标
    'gross_margin',         # 销售毛利率
    'grossprofit_margin',   # 销售毛利率(与gross_margin相同)
    'cogs_of_sales',        # 销售成本率
    'expense_of_sales',     # 销售期间费用率
    'profit_to_gr',         # 净利润/营业总收入
    'saleexp_to_gr',        # 销售费用/营业总收入
    'adminexp_of_gr',       # 管理费用/营业总收入
    'finaexp_of_gr',        # 财务费用/营业总收入
    'impai_ttm',            # 资产减值损失/营业总收入
    'gc_of_gr',             # 营业总成本/营业总收入
    'op_of_gr',             # 营业利润/营业总收入
    'ebit_of_gr',           # 息税前利润/营业总收入
    'netprofit_margin',     # 销售净利率
    'netprofit_of_gr',      # 归属母公司股东的净利润/营业总收入
    
    # 现金流指标
    'ocf_yoy',              # 经营活动产生的现金流量净额同比增长率(%)
    'bps',                  # 每股净资产
    'ocfps',                # 每股经营活动产生的现金流量净额
    'retainedps',           # 每股留存收益
    'cfps',                 # 每股现金流量净额
    'ebit_ps',              # 每股息税前利润
    'fcff_ps',              # 每股企业自由现金流量
    'fcfe_ps',              # 每股股东自由现金流量
    
    # 杜邦分析指标
    'sale_npm',             # 销售净利率(杜邦分析)
    'dupon_ebit',           # EBIT/利润总额
    'dupon_tax_rate',       # 税收负担率
    'dupon_int_rate',       # 利息负担率
    'dupon_roe',            # 净资产收益率(杜邦)
    'dupon_roa',            # 总资产净利率(杜邦)
    'equity_yoy',           # 净资产同比增长率
    'asset_yoy',            # 总资产同比增长率
    'revenue_yoy',          # 营业收入同比增长率(%)
    'profit_yoy',           # 营业利润同比增长率(%)
    'netprofit_yoy',        # 归母净利润同比增长率(%)
    'dt_netprofit_yoy',     # 归母净利润-扣除非经常性损益同比增长率(%)
    'or_yoy',               # 营业收入同比增长率(%)
    'ebit_yoy',             # 息税前利润同比增长率(%)
    'profit_to_op',         # 利润总额/营业收入
    'ta_yoy',               # 总资产同比增长率
    'eqt_yoy',              # 净资产同比增长率
    'tr_yoy',               # 营业总收入同比增长率
    
    # 单季度财务指标
    'q_opincome',           # 单季度经营活动净收益
    'q_investincome',       # 单季度价值变动净收益
    'q_dtprofit',           # 单季度扣除非经常损益后的净利润
    'q_eps',                # 单季度每股收益
    'q_netprofit_margin',   # 单季度销售净利率
    'q_gsprofit_margin',    # 单季度销售毛利率
    'q_exp_to_sales',       # 单季度销售期间费用率
    'q_profit_to_gr',       # 单季度净利润/营业总收入
    'q_saleexp_to_gr',      # 单季度销售费用/营业总收入
    'q_adminexp_to_gr',     # 单季度管理费用/营业总收入
    'q_finaexp_to_gr',      # 单季度财务费用/营业总收入
    'q_impair_to_gr_ttm',   # 单季度资产减值损失/营业总收入
    'q_gc_to_gr',           # 单季度营业总成本/营业总收入
    'q_op_to_gr',           # 单季度营业利润/营业总收入
    'q_roe',                # 单季度净资产收益率ROE
    'q_dt_roe',             # 单季度净资产收益率ROE(扣除非经常损益)
    'q_npta',               # 单季度总资产净利润
    'q_opincome_to_ebt',    # 单季度经营活动净收益/利润总额
    'q_investincome_to_ebt', # 单季度价值变动净收益/利润总额
    'q_dtprofit_to_profit',  # 单季度扣除非经常损益后的净利润/净利润
    'q_salecash_to_or',     # 单季度销售商品提供劳务收到的现金/营业收入
    'q_ocf_to_sales',       # 单季度经营活动产生的现金流量净额/营业收入
    'q_ocf_to_or',          # 单季度经营活动产生的现金流量净额/营业收入
    'basic_eps_yoy',        # 基本每股收益同比增长率(%)
    'dt_eps_yoy',           # 稀释每股收益同比增长率(%)
    'cfps_yoy',             # 每股经营活动产生的现金流量净额同比增长率(%)
    
    # 市场表现指标
    'op_yoy',               # 营业利润同比增长率(%)
    'ebt_yoy',              # 利润总额同比增长率(%)
    'netprofit_margin_yoy', # 销售净利率同比增长率(%)
    'debt_to_eqt',          # 产权比率
    'turn_days',            # 营业周期
    'roa_yearly',           # 年化总资产净利率
    'roa_dp',               # 总资产净利率(杜邦分析)
    'fixed_assets',         # 固定资产合计
    'profit_prefin_exp',    # 扣除财务费用前营业利润
    'non_op_profit',        # 非营业利润
    'op_to_ebt',            # 营业利润/利润总额
    'nop_to_ebt',           # 非营业利润/利润总额
    'ocf_to_profit',        # 经营活动产生的现金流量净额/营业利润
    'cash_to_liqdebt',      # 货币资金/流动负债
    'cash_to_liqdebt_withinterest', # 货币资金/带息流动负债
    'op_to_liqdebt',        # 营业利润/流动负债
    'op_to_debt',           # 营业利润/负债合计
    'roic_yearly',          # 年化投入资本回报率
    'total_fa_trun',        # 固定资产合计周转率
    'profit_to_op',         # 利润总额/营业收入
    'q_saleexp_to_gr_ttm',  # 单季度销售费用/营业总收入(TTM)
    'q_gc_to_gr_ttm',       # 单季度营业总成本/营业总收入(TTM)
    'q_op_to_gr_ttm',       # 单季度营业利润/营业总收入(TTM)
    'q_roe_ttm',            # 单季度净资产收益率(TTM)
    'q_dt_roe_ttm',         # 单季度净资产收益率(扣除非经常性损益,TTM)
    'q_ocf_to_or_ttm',      # 单季度经营活动产生的现金流量净额/营业收入(TTM)
]


# ============================================================================
# 所有财务字段汇总（用于因子系统识别）
# ============================================================================

# 合并所有财务字段（去重）
ALL_FINANCIAL_FIELDS = sorted(list(set(
    INCOME_FIELDS + 
    BALANCE_FIELDS + 
    CASHFLOW_FIELDS + 
    INDICATOR_FIELDS
)))

# 按报表分类的字典（方便按需获取）
FINANCIAL_FIELDS_BY_TYPE = {
    'income': INCOME_FIELDS,
    'balance': BALANCE_FIELDS,
    'cashflow': CASHFLOW_FIELDS,
    'indicator': INDICATOR_FIELDS,
}

# 数据表映射
COLLECTION_MAPPING = {
    'income': 'financial_income',
    'balance': 'financial_balance',
    'cashflow': 'financial_cashflow',
    'indicator': 'financial_indicator',
}


def get_financial_fields(data_type=None):
    """
    获取财务字段列表
    
    Args:
        data_type: 数据类型，可选值：'income', 'balance', 'cashflow', 'indicator'
                  None 表示返回所有财务字段
    
    Returns:
        list: 字段列表
    """
    if data_type is None:
        return ALL_FINANCIAL_FIELDS
    return FINANCIAL_FIELDS_BY_TYPE.get(data_type, [])


def is_financial_field(field_name):
    """
    判断字段是否是财务字段
    
    Args:
        field_name: 字段名
    
    Returns:
        bool: True表示是财务字段
    """
    return field_name in ALL_FINANCIAL_FIELDS


def get_collection_name(data_type):
    """
    根据数据类型获取集合名称
    
    Args:
        data_type: 数据类型
    
    Returns:
        str: 集合名称
    """
    return COLLECTION_MAPPING.get(data_type)


if __name__ == '__main__':
    # 打印统计信息
    print("="*80)
    print("财务字段统计")
    print("="*80)
    print(f"利润表字段数: {len(INCOME_FIELDS)}")
    print(f"资产负债表字段数: {len(BALANCE_FIELDS)}")
    print(f"现金流量表字段数: {len(CASHFLOW_FIELDS)}")
    print(f"财务指标字段数: {len(INDICATOR_FIELDS)}")
    print(f"总计（去重后）: {len(ALL_FINANCIAL_FIELDS)}")
    print("="*80)


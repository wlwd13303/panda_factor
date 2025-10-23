import { useState, useEffect } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import {
  Card,
  Form,
  Input,
  Button,
  Space,
  message,
  DatePicker,
  Switch,
  Select,
  InputNumber,
  Row,
  Col,
  Divider,
} from 'antd'
import { SaveOutlined, PlayCircleOutlined, ArrowLeftOutlined } from '@ant-design/icons'
import dayjs, { Dayjs } from 'dayjs'
import {
  createFactor,
  updateFactor,
  queryFactor,
  runFactor,
  type CreateFactorRequest,
} from '@/api/userFactor'
import './index.css'

const { TextArea } = Input
const { Option } = Select

const defaultCode = `# 定义因子计算逻辑
def calculate_factor(data):
    """
    计算因子值
    
    参数:
        data: DataFrame, 包含股票的行情数据
        
    返回:
        Series, 因子值
    """
    # 示例：计算20日动量
    returns = data['close'].pct_change(20)
    return returns

# 返回因子函数
factor_func = calculate_factor
`

const FactorEditor = () => {
  const navigate = useNavigate()
  const { id } = useParams<{ id: string }>()
  const [form] = Form.useForm()
  const [loading, setLoading] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [code, setCode] = useState(defaultCode)

  const isNew = id === 'new'
  const userId = 'default_user'

  useEffect(() => {
    if (!isNew && id) {
      loadFactor()
    }
  }, [id])

  const loadFactor = async () => {
    if (!id) return
    
    setLoading(true)
    try {
      const factor = await queryFactor(id)
      
      // 填充表单
      form.setFieldsValue({
        name: factor.name,
        factor_name: factor.factor_name,
        describe: factor.describe,
        tags: factor.tags,
        is_persistent: factor.is_persistent,
        factor_start_day: factor.factor_start_day ? dayjs(factor.factor_start_day) : dayjs(),
        start_date: factor.params.start_date ? dayjs(factor.params.start_date) : dayjs().subtract(1, 'year'),
        end_date: factor.params.end_date ? dayjs(factor.params.end_date) : dayjs(),
        stock_pool: factor.params.stock_pool,
        adjustment_cycle: factor.params.adjustment_cycle,
        group_number: factor.params.group_number,
        factor_direction: factor.params.factor_direction,
        include_st: factor.params.include_st,
        extreme_value_processing: factor.params.extreme_value_processing,
      })
      
      setCode(factor.code)
    } catch (error: any) {
      message.error('加载因子失败')
      console.error(error)
    } finally {
      setLoading(false)
    }
  }

  const handleSave = async () => {
    try {
      const values = await form.validateFields()
      
      const factorData: CreateFactorRequest = {
        user_id: userId,
        name: values.name,
        factor_name: values.factor_name || `factor_${Date.now()}`,
        factor_type: 'custom',
        is_persistent: values.is_persistent || false,
        cron: '',
        factor_start_day: values.factor_start_day.format('YYYY-MM-DD'),
        code: code,
        code_type: 'python',
        status: 0,
        describe: values.describe || '',
        tags: values.tags || '',
        params: {
          start_date: values.start_date.format('YYYY-MM-DD'),
          end_date: values.end_date.format('YYYY-MM-DD'),
          adjustment_cycle: values.adjustment_cycle || 1,
          stock_pool: values.stock_pool || '000300',
          factor_direction: values.factor_direction !== false,
          group_number: values.group_number || 5,
          include_st: values.include_st || false,
          extreme_value_processing: values.extreme_value_processing || '标准差',
        },
      }

      setSubmitting(true)
      
      if (isNew) {
        const response: any = await createFactor(factorData)
        message.success('因子创建成功')
        navigate(`/factor-editor/${response.data.factor_id}`)
      } else if (id) {
        await updateFactor(id, factorData)
        message.success('因子保存成功')
      }
    } catch (error: any) {
      message.error(error.message || '保存失败')
    } finally {
      setSubmitting(false)
    }
  }

  const handleRunAndSave = async () => {
    try {
      await handleSave()
      
      if (!isNew && id) {
        const response: any = await runFactor(id)
        message.success('因子分析任务已启动')
        
        if (response.task_id) {
          navigate(`/factor-result/${response.task_id}`)
        }
      }
    } catch (error: any) {
      message.error(error.message || '运行失败')
    }
  }

  return (
    <div className="factor-editor-page">
      <Card
        title={
          <Space>
            <Button
              icon={<ArrowLeftOutlined />}
              onClick={() => navigate('/factor-list')}
            >
              返回
            </Button>
            <span>{isNew ? '新建因子' : '编辑因子'}</span>
          </Space>
        }
        extra={
          <Space>
            <Button
              icon={<SaveOutlined />}
              onClick={handleSave}
              loading={submitting}
            >
              保存
            </Button>
            <Button
              type="primary"
              icon={<PlayCircleOutlined />}
              onClick={handleRunAndSave}
              loading={submitting}
            >
              保存并运行
            </Button>
          </Space>
        }
        loading={loading}
      >
        <Form
          form={form}
          layout="vertical"
          initialValues={{
            is_persistent: false,
            factor_start_day: dayjs(),
            start_date: dayjs().subtract(1, 'year'),
            end_date: dayjs(),
            stock_pool: '000300',
            adjustment_cycle: 1,
            group_number: 5,
            factor_direction: true,
            include_st: false,
            extreme_value_processing: '标准差',
          }}
        >
          {/* 基本信息 */}
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                label="因子名称"
                name="name"
                rules={[{ required: true, message: '请输入因子名称' }]}
              >
                <Input placeholder="例如：动量因子" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="因子标识符" name="factor_name">
                <Input placeholder="自动生成" />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item label="因子描述" name="describe">
            <TextArea rows={2} placeholder="描述因子的计算逻辑和用途" />
          </Form.Item>

          <Row gutter={16}>
            <Col span={12}>
              <Form.Item label="标签" name="tags">
                <Input placeholder="用逗号分隔，如: 动量,技术" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="持久化" name="is_persistent" valuePropName="checked">
                <Switch />
              </Form.Item>
            </Col>
          </Row>

          <Divider>回测参数</Divider>

          {/* 回测参数 */}
          <Row gutter={16}>
            <Col span={8}>
              <Form.Item
                label="开始日期"
                name="start_date"
                rules={[{ required: true, message: '请选择开始日期' }]}
              >
                <DatePicker style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item
                label="结束日期"
                name="end_date"
                rules={[{ required: true, message: '请选择结束日期' }]}
              >
                <DatePicker style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="因子起始日" name="factor_start_day">
                <DatePicker style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={16}>
            <Col span={8}>
              <Form.Item label="股票池" name="stock_pool">
                <Select>
                  <Option value="000300">沪深300</Option>
                  <Option value="000905">中证500</Option>
                  <Option value="000852">中证1000</Option>
                  <Option value="all">全市场</Option>
                </Select>
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="调仓周期（天）" name="adjustment_cycle">
                <InputNumber min={1} max={365} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="分组数" name="group_number">
                <InputNumber min={3} max={10} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={16}>
            <Col span={8}>
              <Form.Item label="因子方向（正向）" name="factor_direction" valuePropName="checked">
                <Switch checkedChildren="正向" unCheckedChildren="反向" />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="包含ST股票" name="include_st" valuePropName="checked">
                <Switch />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="极值处理" name="extreme_value_processing">
                <Select>
                  <Option value="标准差">标准差</Option>
                  <Option value="MAD">MAD</Option>
                  <Option value="分位数">分位数</Option>
                </Select>
              </Form.Item>
            </Col>
          </Row>

          <Divider>因子代码</Divider>

          {/* 代码编辑器 */}
          <div className="code-editor-wrapper">
            <TextArea
              value={code}
              onChange={(e) => setCode(e.target.value)}
              placeholder="请输入Python代码"
              rows={20}
              style={{ fontFamily: 'Consolas, Monaco, monospace', fontSize: '13px' }}
            />
          </div>
        </Form>
      </Card>
    </div>
  )
}

export default FactorEditor


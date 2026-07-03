import React, { useState, useEffect, useCallback } from 'react'
import {
  Table,
  Tag,
  Button,
  Modal,
  Form,
  Input,
  Select,
  Switch,
  Space,
  Typography,
  message,
  Popconfirm,
  Tooltip,
  Spin,
  Divider,
  Row,
  Col,
  Alert
} from 'antd'
import {
  PlusOutlined,
  DeleteOutlined,
  RobotOutlined,
  ReloadOutlined,
  EditOutlined
} from '@ant-design/icons'
import dayjs from 'dayjs'
import { api } from '../api/client.js'

const { Title, Text, Paragraph } = Typography
const { TextArea } = Input

const SEVERITY_COLORS = {
  CRITICAL: 'magenta',
  HIGH: 'red',
  MEDIUM: 'orange',
  LOW: 'blue',
  INFO: 'default'
}

export default function RuleManagement() {
  const [rules, setRules] = useState([])
  const [sources, setSources] = useState([])
  const [loading, setLoading] = useState(false)
  const [modalOpen, setModalOpen] = useState(false)
  const [form] = Form.useForm()

  // AI generation state
  const [aiDescription, setAiDescription] = useState('')
  const [aiLoading, setAiLoading] = useState(false)
  const [aiResult, setAiResult] = useState(null)
  const [aiError, setAiError] = useState(null)

  const [toggleLoading, setToggleLoading] = useState({})
  const [deleteLoading, setDeleteLoading] = useState({})
  const [submitLoading, setSubmitLoading] = useState(false)

  const fetchRules = useCallback(async () => {
    setLoading(true)
    try {
      const data = await api.getRules()
      setRules(Array.isArray(data) ? data : [])
    } catch (err) {
      message.error(`Failed to load rules: ${err.message}`)
    } finally {
      setLoading(false)
    }
  }, [])

  const fetchSources = useCallback(async () => {
    try {
      const data = await api.getEventSources()
      setSources(Array.isArray(data) ? data : [])
    } catch {
      // non-fatal — sources list is optional for rule creation
    }
  }, [])

  useEffect(() => {
    fetchRules()
    fetchSources()
  }, [fetchRules, fetchSources])

  const handleToggle = async (id, enabled) => {
    setToggleLoading((prev) => ({ ...prev, [id]: true }))
    try {
      await api.updateRule(id, { enabled })
      message.success(`Rule ${enabled ? 'enabled' : 'disabled'}`)
      setRules((prev) =>
        prev.map((r) => (r.id === id ? { ...r, enabled } : r))
      )
    } catch (err) {
      message.error(`Failed to update rule: ${err.message}`)
    } finally {
      setToggleLoading((prev) => ({ ...prev, [id]: false }))
    }
  }

  const handleDelete = async (id) => {
    setDeleteLoading((prev) => ({ ...prev, [id]: true }))
    try {
      await api.deleteRule(id)
      message.success('Rule deleted')
      setRules((prev) => prev.filter((r) => r.id !== id))
    } catch (err) {
      message.error(`Failed to delete rule: ${err.message}`)
    } finally {
      setDeleteLoading((prev) => ({ ...prev, [id]: false }))
    }
  }

  const openModal = () => {
    form.resetFields()
    setAiDescription('')
    setAiResult(null)
    setAiError(null)
    setModalOpen(true)
  }

  const closeModal = () => {
    setModalOpen(false)
  }

  const handleGenerateCel = async () => {
    const sourceId = form.getFieldValue('sourceId')
    if (!sourceId) {
      message.warning('Please select a Source before generating CEL')
      return
    }
    if (!aiDescription.trim()) {
      message.warning('Please enter a description for AI generation')
      return
    }
    setAiLoading(true)
    setAiResult(null)
    setAiError(null)
    try {
      const result = await api.generateCel(sourceId, aiDescription)
      setAiResult(result)
      form.setFieldsValue({
        celExpression: result.celExpression,
        celSummary: result.celSummary
      })
      message.success('CEL expression generated successfully')
    } catch (err) {
      setAiError(err.message)
    } finally {
      setAiLoading(false)
    }
  }

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields()
      setSubmitLoading(true)
      await api.createRule(values)
      message.success('Rule created successfully')
      closeModal()
      fetchRules()
    } catch (err) {
      if (err?.errorFields) return // validation error, form already shows it
      message.error(`Failed to create rule: ${err.message}`)
    } finally {
      setSubmitLoading(false)
    }
  }

  const columns = [
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
      width: 200,
      render: (text) => <Text strong>{text}</Text>
    },
    {
      title: 'Source',
      dataIndex: 'sourceId',
      key: 'sourceId',
      width: 160,
      render: (val) => (
        <Text code style={{ fontSize: 12 }}>
          {val}
        </Text>
      )
    },
    {
      title: 'Event Type',
      dataIndex: 'triggerEventType',
      key: 'triggerEventType',
      width: 130,
      render: (val) => <Tag>{val || '—'}</Tag>
    },
    {
      title: 'CEL Expression',
      dataIndex: 'celExpression',
      key: 'celExpression',
      width: 240,
      render: (expr) =>
        expr ? (
          <Tooltip title={expr} overlayStyle={{ maxWidth: 500 }}>
            <Text
              code
              style={{
                fontSize: 12,
                display: 'block',
                maxWidth: 220,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap'
              }}
            >
              {expr}
            </Text>
          </Tooltip>
        ) : (
          <Text type="secondary">—</Text>
        )
    },
    {
      title: 'Severity',
      dataIndex: 'severity',
      key: 'severity',
      width: 100,
      render: (sev) => (
        <Tag color={SEVERITY_COLORS[sev] || 'default'} style={{ fontWeight: 600 }}>
          {sev || '—'}
        </Tag>
      )
    },
    {
      title: 'Enabled',
      dataIndex: 'enabled',
      key: 'enabled',
      width: 90,
      render: (enabled, record) => (
        <Switch
          checked={enabled}
          loading={!!toggleLoading[record.id]}
          onChange={(val) => handleToggle(record.id, val)}
          size="small"
        />
      )
    },
    {
      title: 'Created',
      dataIndex: 'createdAt',
      key: 'createdAt',
      width: 140,
      render: (ts) =>
        ts ? (
          <Text type="secondary" style={{ fontSize: 12 }}>
            {dayjs(ts).format('MMM D, YYYY')}
          </Text>
        ) : (
          '—'
        )
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 90,
      render: (_, record) => (
        <Popconfirm
          title="Delete this rule?"
          description="This action cannot be undone."
          onConfirm={() => handleDelete(record.id)}
          okText="Delete"
          okButtonProps={{ danger: true }}
        >
          <Button
            size="small"
            danger
            icon={<DeleteOutlined />}
            loading={!!deleteLoading[record.id]}
          >
            Delete
          </Button>
        </Popconfirm>
      )
    }
  ]

  return (
    <div>
      <Row justify="space-between" align="middle" style={{ marginBottom: 20 }}>
        <Col>
          <Title level={3} style={{ margin: 0 }}>
            Rule Management
          </Title>
        </Col>
        <Col>
          <Space>
            <Button icon={<ReloadOutlined />} onClick={fetchRules} loading={loading}>
              Refresh
            </Button>
            <Button type="primary" icon={<PlusOutlined />} onClick={openModal}>
              New Rule
            </Button>
          </Space>
        </Col>
      </Row>

      <Table
        dataSource={rules}
        columns={columns}
        rowKey="id"
        loading={loading}
        size="small"
        pagination={{ pageSize: 20, showSizeChanger: true, showTotal: (t) => `${t} rules` }}
        expandable={{
          expandedRowRender: (record) => (
            <div style={{ padding: '8px 0' }}>
              {record.description && (
                <Paragraph style={{ marginBottom: 8 }}>
                  <Text strong>Description: </Text>
                  {record.description}
                </Paragraph>
              )}
              {record.celSummary && (
                <Paragraph style={{ marginBottom: 8 }}>
                  <Text strong>CEL Summary: </Text>
                  {record.celSummary}
                </Paragraph>
              )}
              <Row gutter={24}>
                <Col>
                  <Text strong>Resolution Mode: </Text>
                  <Tag>{record.resolutionMode || '—'}</Tag>
                </Col>
                <Col>
                  <Text strong>Entity Dimension Field: </Text>
                  <Text code>{record.entityDimensionField || '—'}</Text>
                </Col>
              </Row>
            </div>
          )
        }}
      />

      <Modal
        title={
          <Space>
            <EditOutlined />
            Create New Rule
          </Space>
        }
        open={modalOpen}
        onCancel={closeModal}
        width={720}
        footer={
          <Space>
            <Button onClick={closeModal}>Cancel</Button>
            <Button type="primary" onClick={handleSubmit} loading={submitLoading}>
              Create Rule
            </Button>
          </Space>
        }
        destroyOnClose
      >
        <Form form={form} layout="vertical" initialValues={{ entityDimensionField: 'player_id', enabled: true, resolutionMode: 'MANUAL', severity: 'MEDIUM' }}>
          <Row gutter={16}>
            <Col span={14}>
              <Form.Item
                name="name"
                label="Rule Name"
                rules={[{ required: true, message: 'Rule name is required' }]}
              >
                <Input placeholder="e.g. Large Deposit Alert" />
              </Form.Item>
            </Col>
            <Col span={10}>
              <Form.Item name="severity" label="Severity">
                <Select
                  options={[
                    { value: 'INFO', label: <Tag color="default">INFO</Tag> },
                    { value: 'LOW', label: <Tag color="blue">LOW</Tag> },
                    { value: 'MEDIUM', label: <Tag color="orange">MEDIUM</Tag> },
                    { value: 'HIGH', label: <Tag color="red">HIGH</Tag> },
                    { value: 'CRITICAL', label: <Tag color="magenta">CRITICAL</Tag> }
                  ]}
                />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item name="description" label="Description">
            <TextArea rows={2} placeholder="Human-readable description of what this rule detects" />
          </Form.Item>

          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                name="sourceId"
                label="Event Source"
                rules={[{ required: true, message: 'Source is required' }]}
              >
                <Select
                  placeholder="Select source"
                  showSearch
                  options={
                    sources.length > 0
                      ? sources.map((s) => ({
                          value: s.id,
                          label: s.displayName || s.id
                        }))
                      : []
                  }
                  notFoundContent={
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      No sources loaded
                    </Text>
                  }
                />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="triggerEventType" label="Trigger Event Type">
                <Input placeholder="e.g. deposit, login, bet" />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="entityDimensionField" label="Entity Dimension Field">
                <Input placeholder="player_id" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="resolutionMode" label="Resolution Mode">
                <Select
                  options={[
                    { value: 'MANUAL', label: 'MANUAL' },
                    { value: 'AUTO', label: 'AUTO' }
                  ]}
                />
              </Form.Item>
            </Col>
          </Row>

          <Divider orientation="left" orientationMargin={0} style={{ margin: '4px 0 12px' }}>
            <Space>
              <RobotOutlined style={{ color: '#1677ff' }} />
              <Text style={{ fontSize: 13, color: '#1677ff' }}>AI-Assisted CEL Generation</Text>
            </Space>
          </Divider>

          <div
            style={{
              background: '#f0f5ff',
              border: '1px solid #adc6ff',
              borderRadius: 8,
              padding: '12px 16px',
              marginBottom: 12
            }}
          >
            <Text style={{ fontSize: 13, display: 'block', marginBottom: 8 }}>
              Describe what you want to detect in plain English, then generate a CEL expression automatically.
            </Text>
            <Row gutter={8} align="middle">
              <Col flex="auto">
                <Input
                  placeholder='e.g. "Alert when a player deposits more than $500 in a single transaction"'
                  value={aiDescription}
                  onChange={(e) => setAiDescription(e.target.value)}
                  onPressEnter={handleGenerateCel}
                />
              </Col>
              <Col>
                <Button
                  type="primary"
                  icon={aiLoading ? <Spin size="small" /> : <RobotOutlined />}
                  onClick={handleGenerateCel}
                  disabled={aiLoading}
                  style={{ minWidth: 100 }}
                >
                  {aiLoading ? 'Generating…' : 'Generate'}
                </Button>
              </Col>
            </Row>

            {aiError && (
              <Alert
                type="error"
                message={`Generation failed: ${aiError}`}
                showIcon
                style={{ marginTop: 10 }}
                closable
                onClose={() => setAiError(null)}
              />
            )}

            {aiResult && (
              <Alert
                type="success"
                message="CEL expression generated — fields below have been populated."
                showIcon
                style={{ marginTop: 10 }}
                closable
                onClose={() => setAiResult(null)}
              />
            )}
          </div>

          <Form.Item
            name="celExpression"
            label="CEL Expression"
            rules={[{ required: true, message: 'CEL expression is required' }]}
          >
            <TextArea
              rows={3}
              placeholder='e.g. payload.amount > 500.0 && payload.currency == "USD"'
              style={{ fontFamily: 'monospace', fontSize: 13 }}
            />
          </Form.Item>

          <Form.Item name="celSummary" label="CEL Summary">
            <TextArea
              rows={2}
              placeholder="Human-readable summary of the CEL expression (auto-populated by AI)"
            />
          </Form.Item>

          <Form.Item name="enabled" label="Enable Rule" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}

import React, { useState, useEffect, useCallback } from 'react'
import {
  Table,
  Tag,
  Button,
  Select,
  Space,
  Typography,
  message,
  Tooltip,
  Badge,
  Row,
  Col,
  Statistic,
  Card
} from 'antd'
import {
  ReloadOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  ExclamationCircleOutlined
} from '@ant-design/icons'
import dayjs from 'dayjs'
import { api } from '../api/client.js'

const { Title, Text } = Typography

const STATUS_COLORS = {
  OPEN: 'error',
  ACKNOWLEDGED: 'warning',
  RESOLVED: 'success'
}

const STATUS_TAG_COLORS = {
  OPEN: 'red',
  ACKNOWLEDGED: 'orange',
  RESOLVED: 'green'
}

const SEVERITY_COLORS = {
  CRITICAL: 'magenta',
  HIGH: 'red',
  MEDIUM: 'orange',
  LOW: 'blue',
  INFO: 'default'
}

export default function AlertDashboard() {
  const [alerts, setAlerts] = useState([])
  const [loading, setLoading] = useState(false)
  const [actionLoading, setActionLoading] = useState({})
  const [statusFilter, setStatusFilter] = useState(null)

  const fetchAlerts = useCallback(async () => {
    setLoading(true)
    try {
      const params = {}
      if (statusFilter) params.status = statusFilter
      const data = await api.getAlerts(params)
      setAlerts(Array.isArray(data) ? data : [])
    } catch (err) {
      message.error(`Failed to load alerts: ${err.message}`)
    } finally {
      setLoading(false)
    }
  }, [statusFilter])

  useEffect(() => {
    fetchAlerts()
  }, [fetchAlerts])

  useEffect(() => {
    const interval = setInterval(fetchAlerts, 5000)
    return () => clearInterval(interval)
  }, [fetchAlerts])

  const handleAcknowledge = async (id) => {
    setActionLoading((prev) => ({ ...prev, [`ack-${id}`]: true }))
    try {
      await api.acknowledgeAlert(id)
      message.success('Alert acknowledged')
      fetchAlerts()
    } catch (err) {
      message.error(`Failed to acknowledge: ${err.message}`)
    } finally {
      setActionLoading((prev) => ({ ...prev, [`ack-${id}`]: false }))
    }
  }

  const handleResolve = async (id) => {
    setActionLoading((prev) => ({ ...prev, [`res-${id}`]: true }))
    try {
      await api.resolveAlert(id)
      message.success('Alert resolved')
      fetchAlerts()
    } catch (err) {
      message.error(`Failed to resolve: ${err.message}`)
    } finally {
      setActionLoading((prev) => ({ ...prev, [`res-${id}`]: false }))
    }
  }

  const openCount = alerts.filter((a) => a.status === 'OPEN').length
  const ackCount = alerts.filter((a) => a.status === 'ACKNOWLEDGED').length
  const resolvedCount = alerts.filter((a) => a.status === 'RESOLVED').length

  const columns = [
    {
      title: 'Rule Name',
      dataIndex: 'ruleName',
      key: 'ruleName',
      width: 200,
      render: (text) => <Text strong>{text}</Text>
    },
    {
      title: 'Entity',
      dataIndex: 'entityDimensionValue',
      key: 'entityDimensionValue',
      width: 150,
      render: (val) => (
        <Text code style={{ fontSize: 12 }}>
          {val}
        </Text>
      )
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      width: 130,
      render: (status) => (
        <Tag color={STATUS_TAG_COLORS[status] || 'default'} style={{ fontWeight: 600 }}>
          {status}
        </Tag>
      )
    },
    {
      title: 'Severity',
      dataIndex: 'severity',
      key: 'severity',
      width: 110,
      render: (sev) => (
        <Tag color={SEVERITY_COLORS[sev] || 'default'} style={{ fontWeight: 600 }}>
          {sev}
        </Tag>
      )
    },
    {
      title: 'Fired At',
      dataIndex: 'firedAt',
      key: 'firedAt',
      width: 170,
      render: (ts) =>
        ts ? (
          <Tooltip title={dayjs(ts).format('YYYY-MM-DD HH:mm:ss')}>
            <Text type="secondary" style={{ fontSize: 13 }}>
              {dayjs(ts).format('MMM D, HH:mm:ss')}
            </Text>
          </Tooltip>
        ) : (
          '—'
        ),
      sorter: (a, b) => new Date(a.firedAt) - new Date(b.firedAt),
      defaultSortOrder: 'descend'
    },
    {
      title: 'Acknowledged At',
      dataIndex: 'acknowledgedAt',
      key: 'acknowledgedAt',
      width: 160,
      render: (ts) =>
        ts ? (
          <Text type="secondary" style={{ fontSize: 13 }}>
            {dayjs(ts).format('MMM D, HH:mm:ss')}
          </Text>
        ) : (
          <Text type="secondary">—</Text>
        )
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 200,
      render: (_, record) => (
        <Space size="small">
          {record.status === 'OPEN' && (
            <Button
              size="small"
              icon={<CheckCircleOutlined />}
              loading={actionLoading[`ack-${record.id}`]}
              onClick={() => handleAcknowledge(record.id)}
              style={{ borderColor: '#fa8c16', color: '#fa8c16' }}
            >
              Acknowledge
            </Button>
          )}
          {(record.status === 'OPEN' || record.status === 'ACKNOWLEDGED') && (
            <Button
              size="small"
              type="primary"
              icon={<CloseCircleOutlined />}
              loading={actionLoading[`res-${record.id}`]}
              onClick={() => handleResolve(record.id)}
              style={{ background: '#52c41a', borderColor: '#52c41a' }}
            >
              Resolve
            </Button>
          )}
          {record.status === 'RESOLVED' && (
            <Text type="secondary" style={{ fontSize: 12 }}>
              Resolved
            </Text>
          )}
        </Space>
      )
    }
  ]

  return (
    <div>
      <Row justify="space-between" align="middle" style={{ marginBottom: 20 }}>
        <Col>
          <Title level={3} style={{ margin: 0 }}>
            Alert Dashboard
          </Title>
        </Col>
        <Col>
          <Space>
            <Select
              placeholder="Filter by status"
              allowClear
              style={{ width: 180 }}
              value={statusFilter}
              onChange={(val) => setStatusFilter(val || null)}
              options={[
                { value: 'OPEN', label: 'OPEN' },
                { value: 'ACKNOWLEDGED', label: 'ACKNOWLEDGED' },
                { value: 'RESOLVED', label: 'RESOLVED' }
              ]}
            />
            <Button
              icon={<ReloadOutlined />}
              onClick={fetchAlerts}
              loading={loading}
            >
              Refresh
            </Button>
          </Space>
        </Col>
      </Row>

      <Row gutter={16} style={{ marginBottom: 20 }}>
        <Col span={8}>
          <Card size="small">
            <Statistic
              title="Open"
              value={openCount}
              valueStyle={{ color: '#cf1322' }}
              prefix={<ExclamationCircleOutlined />}
            />
          </Card>
        </Col>
        <Col span={8}>
          <Card size="small">
            <Statistic
              title="Acknowledged"
              value={ackCount}
              valueStyle={{ color: '#d46b08' }}
              prefix={<CheckCircleOutlined />}
            />
          </Card>
        </Col>
        <Col span={8}>
          <Card size="small">
            <Statistic
              title="Resolved"
              value={resolvedCount}
              valueStyle={{ color: '#3f8600' }}
              prefix={<CloseCircleOutlined />}
            />
          </Card>
        </Col>
      </Row>

      <Table
        dataSource={alerts}
        columns={columns}
        rowKey="id"
        loading={loading}
        size="small"
        pagination={{ pageSize: 20, showSizeChanger: true, showTotal: (t) => `${t} alerts` }}
        expandable={{
          expandedRowRender: (record) => (
            <div style={{ padding: '8px 0' }}>
              <Row gutter={24}>
                <Col span={12}>
                  <Text strong style={{ display: 'block', marginBottom: 6 }}>
                    Matched Event Snapshot
                  </Text>
                  <pre
                    style={{
                      background: '#f5f5f5',
                      padding: 12,
                      borderRadius: 6,
                      fontSize: 12,
                      overflow: 'auto',
                      maxHeight: 200,
                      border: '1px solid #e8e8e8'
                    }}
                  >
                    {JSON.stringify(record.matchedEventSnapshot, null, 2) || 'null'}
                  </pre>
                </Col>
                <Col span={12}>
                  <Text strong style={{ display: 'block', marginBottom: 6 }}>
                    Aggregation Snapshot
                  </Text>
                  <pre
                    style={{
                      background: '#f5f5f5',
                      padding: 12,
                      borderRadius: 6,
                      fontSize: 12,
                      overflow: 'auto',
                      maxHeight: 200,
                      border: '1px solid #e8e8e8'
                    }}
                  >
                    {JSON.stringify(record.aggregationSnapshot, null, 2) || 'null'}
                  </pre>
                </Col>
              </Row>
            </div>
          )
        }}
        rowClassName={(record) => {
          if (record.status === 'OPEN') return 'alert-row-open'
          return ''
        }}
      />

      <style>{`
        .alert-row-open td {
          background-color: #fff2f0 !important;
        }
        .alert-row-open:hover td {
          background-color: #ffebe8 !important;
        }
      `}</style>
    </div>
  )
}

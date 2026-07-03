import React, { useState, useEffect, useCallback } from 'react'
import {
  Card,
  Table,
  Tag,
  Typography,
  message,
  Row,
  Col,
  Space,
  Button,
  Empty,
  Skeleton,
  Descriptions,
  Badge
} from 'antd'
import {
  DatabaseOutlined,
  ReloadOutlined,
  ApiOutlined,
  TagsOutlined,
  FieldTimeOutlined
} from '@ant-design/icons'
import { api } from '../api/client.js'

const { Title, Text } = Typography

const TYPE_COLORS = {
  DOUBLE: 'blue',
  LONG: 'purple',
  STRING: 'green',
  BOOLEAN: 'orange',
  INT: 'cyan',
  FLOAT: 'geekblue'
}

function PayloadFieldsTable({ fields }) {
  if (!fields || fields.length === 0) {
    return <Text type="secondary" style={{ fontSize: 12 }}>No payload fields defined</Text>
  }
  const columns = [
    {
      title: 'Field Name',
      dataIndex: 'name',
      key: 'name',
      render: (name, record) => (
        <Text code style={{ fontSize: 12 }}>
          {record.payloadPath ? `${name} (← ${record.payloadPath})` : name}
        </Text>
      )
    },
    {
      title: 'Type',
      dataIndex: 'type',
      key: 'type',
      render: (type) => (
        <Tag color={TYPE_COLORS[type] || 'default'} style={{ fontWeight: 600, fontSize: 11 }}>
          {type}
        </Tag>
      )
    }
  ]
  return (
    <Table
      dataSource={fields}
      columns={columns}
      rowKey="name"
      size="small"
      pagination={false}
      style={{ marginTop: 4 }}
    />
  )
}

function SourceCard({ source }) {
  const kafkaTopic = source.kafka?.topic || source.kafka?.topicName || '—'
  const dimensions = source.entity?.dimensions || []
  const payloadFields = source.payloadFields || []

  return (
    <Card
      title={
        <Space>
          <DatabaseOutlined style={{ color: '#1677ff' }} />
          <Text strong style={{ fontSize: 15 }}>
            {source.id}
          </Text>
        </Space>
      }
      extra={
        <Badge
          status="processing"
          color="green"
          text={<Text style={{ fontSize: 12, color: '#52c41a' }}>Active</Text>}
        />
      }
      style={{
        borderRadius: 10,
        boxShadow: '0 2px 8px rgba(0,0,0,0.07)',
        height: '100%'
      }}
      styles={{ body: { paddingTop: 12 } }}
    >
      <Descriptions column={1} size="small" style={{ marginBottom: 12 }}>
        <Descriptions.Item
          label={
            <Space size={4}>
              <TagsOutlined style={{ color: '#8c8c8c' }} />
              <Text type="secondary" style={{ fontSize: 12 }}>
                Display Name
              </Text>
            </Space>
          }
        >
          <Text style={{ fontSize: 13 }}>{source.displayName || source.id}</Text>
        </Descriptions.Item>

        <Descriptions.Item
          label={
            <Space size={4}>
              <ApiOutlined style={{ color: '#8c8c8c' }} />
              <Text type="secondary" style={{ fontSize: 12 }}>
                Kafka Topic
              </Text>
            </Space>
          }
        >
          <Text code style={{ fontSize: 12 }}>
            {kafkaTopic}
          </Text>
        </Descriptions.Item>

        <Descriptions.Item
          label={
            <Space size={4}>
              <FieldTimeOutlined style={{ color: '#8c8c8c' }} />
              <Text type="secondary" style={{ fontSize: 12 }}>
                Dimensions
              </Text>
            </Space>
          }
        >
          <Space wrap size={4}>
            {dimensions.length === 0
              ? <Text type="secondary" style={{ fontSize: 12 }}>—</Text>
              : dimensions.map((d) => (
                  <Tag key={d.name} color="purple" style={{ fontSize: 11 }}>
                    {d.name}
                    <Text type="secondary" style={{ fontSize: 10, marginLeft: 4 }}>
                      ← {d.field}
                    </Text>
                  </Tag>
                ))}
          </Space>
        </Descriptions.Item>
      </Descriptions>

      <div>
        <Text
          strong
          style={{ fontSize: 12, color: '#595959', display: 'block', marginBottom: 4 }}
        >
          Payload Fields ({payloadFields.length})
        </Text>
        <PayloadFieldsTable fields={payloadFields} />
      </div>
    </Card>
  )
}

function PlaceholderCard() {
  return (
    <Card
      style={{
        borderRadius: 10,
        border: '2px dashed #d9d9d9',
        background: '#fafafa',
        height: '100%',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center'
      }}
      styles={{ body: { width: '100%' } }}
    >
      <div style={{ textAlign: 'center', padding: '24px 0' }}>
        <DatabaseOutlined style={{ fontSize: 36, color: '#bfbfbf', display: 'block', marginBottom: 12 }} />
        <Text type="secondary" style={{ fontSize: 13 }}>
          No event sources registered.
        </Text>
        <br />
        <Text type="secondary" style={{ fontSize: 12 }}>
          Register sources via the backend API to see them here.
        </Text>
      </div>
    </Card>
  )
}

export default function EventSourceBrowser() {
  const [sources, setSources] = useState([])
  const [loading, setLoading] = useState(false)

  const fetchSources = useCallback(async () => {
    setLoading(true)
    try {
      const data = await api.getEventSources()
      setSources(Array.isArray(data) ? data : [])
    } catch (err) {
      message.error(`Failed to load event sources: ${err.message}`)
      setSources([])
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchSources()
  }, [fetchSources])

  return (
    <div>
      <Row justify="space-between" align="middle" style={{ marginBottom: 20 }}>
        <Col>
          <Space align="center">
            <Title level={3} style={{ margin: 0 }}>
              Event Sources
            </Title>
            {!loading && (
              <Tag color="blue" style={{ fontSize: 13 }}>
                {sources.length} source{sources.length !== 1 ? 's' : ''}
              </Tag>
            )}
          </Space>
        </Col>
        <Col>
          <Button icon={<ReloadOutlined />} onClick={fetchSources} loading={loading}>
            Refresh
          </Button>
        </Col>
      </Row>

      {loading ? (
        <Row gutter={[20, 20]}>
          {[1, 2, 3].map((i) => (
            <Col key={i} xs={24} sm={24} md={12} lg={8}>
              <Card style={{ borderRadius: 10 }}>
                <Skeleton active paragraph={{ rows: 5 }} />
              </Card>
            </Col>
          ))}
        </Row>
      ) : sources.length === 0 ? (
        <Row gutter={[20, 20]}>
          <Col xs={24} sm={24} md={12} lg={8}>
            <PlaceholderCard />
          </Col>
        </Row>
      ) : (
        <Row gutter={[20, 20]}>
          {sources.map((source) => (
            <Col key={source.id} xs={24} sm={24} md={12} lg={8}>
              <SourceCard source={source} />
            </Col>
          ))}
        </Row>
      )}
    </div>
  )
}

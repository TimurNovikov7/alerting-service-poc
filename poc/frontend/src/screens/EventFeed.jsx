import React, { useState, useEffect, useRef, useCallback } from 'react'
import {
  Table,
  Select,
  Space,
  Typography,
  message,
  Badge,
  Button,
  Tag,
  Tooltip,
  Row,
  Col,
  Statistic
} from 'antd'
import {
  PauseOutlined,
  PlayCircleOutlined,
  ThunderboltFilled,
  ReloadOutlined
} from '@ant-design/icons'
import dayjs from 'dayjs'
import { api } from '../api/client.js'

const { Title, Text } = Typography

const SOURCE_TAG_COLORS = {
  punter_login: 'geekblue',
  withdrawal: 'gold',
  external_bet: 'green'
}

const SOURCE_ROW_COLORS = {
  punter_login: { bg: '#e6f4ff', hover: '#d0ebff' },
  withdrawal: { bg: '#fffbe6', hover: '#fff8d6' },
  external_bet: { bg: '#f6ffed', hover: '#eaffd8' }
}

export default function EventFeed() {
  const [events, setEvents] = useState([])
  const [sources, setSources] = useState([])
  const [sourceFilter, setSourceFilter] = useState(null)
  const [loading, setLoading] = useState(false)
  const [paused, setPaused] = useState(false)
  const intervalRef = useRef(null)

  const fetchEvents = useCallback(async () => {
    if (paused) return
    try {
      const params = { limit: 50 }
      if (sourceFilter) params.sourceId = sourceFilter
      const data = await api.getEvents(params)
      setEvents(Array.isArray(data) ? data : [])
    } catch (err) {
      // Silently fail on background refresh; only show error on manual refresh
    }
  }, [sourceFilter, paused])

  const fetchEventsManual = useCallback(async () => {
    setLoading(true)
    try {
      const params = { limit: 50 }
      if (sourceFilter) params.sourceId = sourceFilter
      const data = await api.getEvents(params)
      setEvents(Array.isArray(data) ? data : [])
    } catch (err) {
      message.error(`Failed to load events: ${err.message}`)
    } finally {
      setLoading(false)
    }
  }, [sourceFilter])

  const fetchSources = useCallback(async () => {
    try {
      const data = await api.getEventSources()
      setSources(Array.isArray(data) ? data : [])
    } catch {
      // non-fatal
    }
  }, [])

  useEffect(() => {
    fetchSources()
  }, [fetchSources])

  useEffect(() => {
    fetchEventsManual()
  }, [sourceFilter]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (paused) {
      if (intervalRef.current) clearInterval(intervalRef.current)
      return
    }
    intervalRef.current = setInterval(fetchEvents, 2000)
    return () => clearInterval(intervalRef.current)
  }, [paused, fetchEvents])

  const sourceOptions = [
    { value: null, label: 'All Sources' },
    ...sources.map((s) => ({ value: s.id, label: s.displayName || s.id }))
  ]

  // Sort events by occurred_at descending
  const displayEvents = [...events].sort(
    (a, b) => new Date(b.occurred_at) - new Date(a.occurred_at)
  )

  const columns = [
    {
      title: 'Source',
      dataIndex: 'source_id',
      key: 'source_id',
      width: 170,
      render: (src) => (
        <Tag
          color={SOURCE_TAG_COLORS[src] || 'default'}
          style={{ fontWeight: 600, fontSize: 12 }}
        >
          {src}
        </Tag>
      )
    },
    {
      title: 'Dimensions',
      key: 'entity',
      width: 180,
      render: (_, record) => {
        const dims = Object.entries(record)
          .filter(([k]) => k.startsWith('dim_'))
          .map(([k, v]) => `${k.replace('dim_', '')}: ${v}`)
          .join(' / ')
        return <Text code style={{ fontSize: 12 }}>{dims || '—'}</Text>
      }
    },
    {
      title: 'Event Type',
      dataIndex: 'event_type',
      key: 'event_type',
      width: 130,
      render: (type) => <Tag>{type}</Tag>
    },
    {
      title: 'Occurred At',
      dataIndex: 'occurred_at',
      key: 'occurred_at',
      width: 180,
      render: (ts) =>
        ts ? (
          <Tooltip title={dayjs(ts).format('YYYY-MM-DD HH:mm:ss.SSS')}>
            <Text type="secondary" style={{ fontSize: 13 }}>
              {dayjs(ts).format('MMM D, HH:mm:ss')}
            </Text>
          </Tooltip>
        ) : (
          '—'
        ),
      sorter: (a, b) => new Date(a.occurred_at) - new Date(b.occurred_at),
      defaultSortOrder: 'descend'
    },
    {
      title: 'Payload',
      dataIndex: 'payload',
      key: 'payload',
      render: (payload) => {
        if (!payload || Object.keys(payload).length === 0) {
          return <Text type="secondary">—</Text>
        }
        const preview = Object.entries(payload)
          .slice(0, 3)
          .map(([k, v]) => `${k}: ${JSON.stringify(v)}`)
          .join(', ')
        const full = JSON.stringify(payload, null, 2)
        return (
          <Tooltip
            title={
              <pre style={{ margin: 0, fontSize: 12, maxWidth: 400, whiteSpace: 'pre-wrap' }}>
                {full}
              </pre>
            }
            overlayStyle={{ maxWidth: 460 }}
          >
            <Text
              style={{
                fontSize: 12,
                color: '#595959',
                cursor: 'pointer',
                maxWidth: 260,
                display: 'block',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap'
              }}
            >
              {preview}
              {Object.keys(payload).length > 3 ? ' …' : ''}
            </Text>
          </Tooltip>
        )
      }
    }
  ]

  return (
    <div>
      <Row justify="space-between" align="middle" style={{ marginBottom: 20 }}>
        <Col>
          <Space align="center">
            <Title level={3} style={{ margin: 0 }}>
              Event Feed
            </Title>
            {paused ? (
              <Tag color="default" style={{ fontWeight: 700, fontSize: 13 }}>
                PAUSED
              </Tag>
            ) : (
              <Badge
                status="processing"
                color="green"
                text={
                  <Text style={{ color: '#52c41a', fontWeight: 700, fontSize: 13 }}>
                    LIVE
                  </Text>
                }
              />
            )}
            <Tag color="blue" style={{ marginLeft: 8 }}>
              {displayEvents.length} events
            </Tag>
          </Space>
        </Col>
        <Col>
          <Space>
            <Select
              placeholder="Filter by source"
              style={{ width: 200 }}
              value={sourceFilter}
              onChange={(val) => setSourceFilter(val || null)}
              options={sourceOptions}
              allowClear
            />
            <Button
              icon={<ReloadOutlined />}
              onClick={fetchEventsManual}
              loading={loading}
            >
              Refresh
            </Button>
            <Button
              icon={paused ? <PlayCircleOutlined /> : <PauseOutlined />}
              onClick={() => setPaused((p) => !p)}
              type={paused ? 'primary' : 'default'}
            >
              {paused ? 'Resume' : 'Pause'}
            </Button>
          </Space>
        </Col>
      </Row>

      <Table
        dataSource={displayEvents}
        columns={columns}
        rowKey={(record, idx) =>
          `${record.source_id}-${record.occurred_at}-${idx}`
        }
        loading={loading}
        size="small"
        pagination={{ pageSize: 50, showSizeChanger: false, showTotal: (t) => `${t} events` }}
        expandable={{
          expandedRowRender: (record) => (
            <div style={{ padding: '8px 0' }}>
              <Text strong style={{ display: 'block', marginBottom: 6 }}>
                Full Payload
              </Text>
              <pre
                style={{
                  background: '#f5f5f5',
                  padding: 12,
                  borderRadius: 6,
                  fontSize: 12,
                  overflow: 'auto',
                  maxHeight: 200,
                  border: '1px solid #e8e8e8',
                  margin: 0
                }}
              >
                {JSON.stringify(record.payload, null, 2) || 'null'}
              </pre>
            </div>
          )
        }}
        rowClassName={(record) => `event-row-${record.source_id}`}
      />

      <style>{`
        ${Object.entries(SOURCE_ROW_COLORS).map(([src, c]) => `
          .event-row-${src} td { background-color: ${c.bg} !important; }
          .event-row-${src}:hover td { background-color: ${c.hover} !important; }
        `).join('')}
      `}</style>
    </div>
  )
}

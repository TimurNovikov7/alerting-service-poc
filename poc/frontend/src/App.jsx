import React from 'react'
import { BrowserRouter, Routes, Route, Link, useLocation } from 'react-router-dom'
import { Layout, Menu, Typography } from 'antd'
import {
  BellOutlined,
  SafetyOutlined,
  ThunderboltOutlined,
  DatabaseOutlined
} from '@ant-design/icons'
import AlertDashboard from './screens/AlertDashboard.jsx'
import RuleManagement from './screens/RuleManagement.jsx'
import EventFeed from './screens/EventFeed.jsx'
import EventSourceBrowser from './screens/EventSourceBrowser.jsx'

const { Sider, Content } = Layout

function NavMenu() {
  const location = useLocation()
  const items = [
    {
      key: '/',
      icon: <BellOutlined />,
      label: <Link to="/">Alerts</Link>
    },
    {
      key: '/rules',
      icon: <SafetyOutlined />,
      label: <Link to="/rules">Rules</Link>
    },
    {
      key: '/events',
      icon: <ThunderboltOutlined />,
      label: <Link to="/events">Event Feed</Link>
    },
    {
      key: '/sources',
      icon: <DatabaseOutlined />,
      label: <Link to="/sources">Event Sources</Link>
    }
  ]
  return (
    <Menu
      theme="dark"
      mode="inline"
      selectedKeys={[location.pathname]}
      items={items}
      style={{ marginTop: 8 }}
    />
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <Layout style={{ minHeight: '100vh' }}>
        <Sider
          width={220}
          style={{
            background: '#141414',
            position: 'fixed',
            height: '100vh',
            left: 0,
            top: 0,
            bottom: 0,
            zIndex: 100
          }}
        >
          <div
            style={{
              padding: '18px 24px',
              color: '#fff',
              fontWeight: 700,
              fontSize: 16,
              letterSpacing: '0.02em',
              borderBottom: '1px solid rgba(255,255,255,0.08)',
              display: 'flex',
              alignItems: 'center',
              gap: 10
            }}
          >
            <BellOutlined style={{ color: '#1677ff', fontSize: 18 }} />
            Events Monitor
          </div>
          <NavMenu />
        </Sider>
        <Layout style={{ marginLeft: 220 }}>
          <Content
            style={{
              margin: 24,
              background: '#fff',
              padding: 24,
              borderRadius: 8,
              minHeight: 'calc(100vh - 48px)'
            }}
          >
            <Routes>
              <Route path="/" element={<AlertDashboard />} />
              <Route path="/rules" element={<RuleManagement />} />
              <Route path="/events" element={<EventFeed />} />
              <Route path="/sources" element={<EventSourceBrowser />} />
            </Routes>
          </Content>
        </Layout>
      </Layout>
    </BrowserRouter>
  )
}

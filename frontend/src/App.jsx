import React, { useState, useEffect } from 'react'
import './App.css'

function formatPayload(payload) {
  if (!payload) return null
  const result = {}
  for (const [key, value] of Object.entries(payload)) {
    if (typeof value === 'string') {
      try {
        result[key] = JSON.parse(value)
      } catch {
        result[key] = value
      }
    } else {
      result[key] = value
    }
  }
  return JSON.stringify(result, null, 2).replace(/\\n/g, '\n')
}

function AuditPanel({ workflowId }) {
  const [audits, setAudits] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch(`/api/workflows/${workflowId}/audits`)
      .then(res => res.json())
      .then(data => {
        setAudits(data)
        setLoading(false)
      })
  }, [workflowId])

  if (loading) return <div className="loading">Loading audits...</div>
  if (audits.length === 0) return <div className="no-data">No audits recorded</div>

  return (
    <table className="audit-table">
      <thead>
        <tr>
          <th>Agent</th>
          <th>Message Type</th>
          <th>Audit Type</th>
          <th>Tokens</th>
          <th>Payload</th>
          <th>Time</th>
        </tr>
      </thead>
      <tbody>
        {audits.map(a => (
          <tr key={a.id}>
            <td>{a.agent_name}</td>
            <td>{a.message_type}</td>
            <td>{a.audit_type}</td>
            <td>{a.total_tokens || '—'}</td>
            <td className="payload-cell">
              {a.payload ? (
                <pre>{formatPayload(a.payload)}</pre>
              ) : '—'}
            </td>
            <td>{new Date(a.created_at).toLocaleString()}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function PromptVersionsPanel({ workflowId }) {
  const [versions, setVersions] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch(`/api/workflows/${workflowId}/prompt-versions`)
      .then(res => res.json())
      .then(data => {
        setVersions(data)
        setLoading(false)
      })
  }, [workflowId])

  if (loading) return <div className="loading">Loading prompt versions...</div>
  if (versions.length === 0) return <div className="no-data">No prompt versions recorded</div>

  return (
    <table className="prompt-versions-table">
      <thead>
        <tr>
          <th>Agent</th>
          <th>Prompt Key</th>
          <th>Type</th>
          <th>Version</th>
        </tr>
      </thead>
      <tbody>
        {versions.map(v => (
          <tr key={`${v.prompt_id}-${v.prompt_version_number}`}>
            <td>{v.agent_name}</td>
            <td className="id-cell">{v.prompt_key}</td>
            <td><span className={`type-badge type-${v.prompt_type}`}>{v.prompt_type}</span></td>
            <td>v{v.prompt_version_number}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function WorkflowPanel({ patientId }) {
  const [workflows, setWorkflows] = useState([])
  const [loading, setLoading] = useState(true)
  const [expandedId, setExpandedId] = useState(null)
  const [expandedPanel, setExpandedPanel] = useState(null)

  const togglePanel = (workflowId, panel) => {
    if (expandedId === workflowId && expandedPanel === panel) {
      setExpandedId(null)
      setExpandedPanel(null)
    } else {
      setExpandedId(workflowId)
      setExpandedPanel(panel)
    }
  }

  useEffect(() => {
    fetch(`/api/patients/${patientId}/workflows`)
      .then(res => res.json())
      .then(data => {
        setWorkflows(data)
        setLoading(false)
      })
  }, [patientId])

  if (loading) return <div className="loading">Loading workflows...</div>
  if (workflows.length === 0) return <div className="no-data">No workflows found</div>

  return (
    <table className="workflow-table">
      <thead>
        <tr>
          <th>Workflow ID</th>
          <th>Status</th>
          <th>Trial Corpus</th>
          <th>Model</th>
          <th>Failure Message</th>
          <th>Content</th>
          <th>Created</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {workflows.map(w => (
          <React.Fragment key={w.id}>
            <tr className={`workflow-row ${expandedId === w.id ? 'selected' : ''}`}>
              <td className="id-cell">{w.id.slice(0, 8)}...</td>
              <td><span className={`status-badge status-${w.status}`}>{w.status}</span></td>
              <td className="id-cell">{w.trial_corpus || '—'}</td>
              <td className="id-cell">{w.model || '—'}</td>
              <td className="failure-cell">{w.failure_message || '—'}</td>
              <td className="content-cell">
                <pre>{(w.content || '').slice(0, 200)}{w.content?.length > 200 ? '...' : ''}</pre>
              </td>
              <td>{new Date(w.created_at).toLocaleString()}</td>
              <td className="workflow-actions">
                <button
                  className={`audit-btn ${expandedId === w.id && expandedPanel === 'audits' ? 'active' : ''}`}
                  onClick={() => togglePanel(w.id, 'audits')}
                >
                  Audits
                </button>
                <button
                  className={`audit-btn ${expandedId === w.id && expandedPanel === 'prompts' ? 'active' : ''}`}
                  onClick={() => togglePanel(w.id, 'prompts')}
                >
                  Prompts
                </button>
              </td>
            </tr>
            {expandedId === w.id && (
              <tr className="audit-row">
                <td colSpan="8">
                  {expandedPanel === 'audits' && <AuditPanel workflowId={w.id} />}
                  {expandedPanel === 'prompts' && <PromptVersionsPanel workflowId={w.id} />}
                </td>
              </tr>
            )}
          </React.Fragment>
        ))}
      </tbody>
    </table>
  )
}

function WorkflowsTab() {
  const [patients, setPatients] = useState([])
  const [loading, setLoading] = useState(true)
  const [expandedPatient, setExpandedPatient] = useState(null)

  useEffect(() => {
    fetch('/api/patients')
      .then(res => res.json())
      .then(data => {
        setPatients(data)
        setLoading(false)
      })
  }, [])

  if (loading) return <div className="loading">Loading...</div>

  return (
    <>
      <p className="subtitle">{patients.length} patients with workflows</p>
      <table className="patient-table">
        <thead>
          <tr>
            <th>Patient ID</th>
            <th>Workflows</th>
            <th>Last Workflow</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {patients.map(p => (
            <React.Fragment key={p.patient_id}>
              <tr className={`patient-row ${expandedPatient === p.patient_id ? 'expanded' : ''}`}>
                <td className="id-cell">{p.patient_id.slice(0, 12)}...</td>
                <td>{p.workflow_count}</td>
                <td>{new Date(p.last_workflow_at).toLocaleString()}</td>
                <td>
                  <button
                    className="view-btn"
                    onClick={() => setExpandedPatient(
                      expandedPatient === p.patient_id ? null : p.patient_id
                    )}
                  >
                    {expandedPatient === p.patient_id ? 'Hide' : 'View Workflows'}
                  </button>
                </td>
              </tr>
              {expandedPatient === p.patient_id && (
                <tr className="workflows-panel-row">
                  <td colSpan="4">
                    <WorkflowPanel patientId={p.patient_id} />
                  </td>
                </tr>
              )}
            </React.Fragment>
          ))}
        </tbody>
      </table>
    </>
  )
}

function PromptHistoryPanel({ promptId }) {
  const [history, setHistory] = useState([])
  const [loading, setLoading] = useState(true)
  const [expandedVersion, setExpandedVersion] = useState(null)

  useEffect(() => {
    fetch(`/api/prompts/${promptId}/history`)
      .then(res => res.json())
      .then(data => {
        setHistory(data)
        setLoading(false)
      })
  }, [promptId])

  if (loading) return <div className="loading">Loading history...</div>
  if (history.length === 0) return <div className="no-data">No previous versions</div>

  return (
    <div className="history-panel">
      <h4 className="history-title">Previous Versions</h4>
      <table className="history-table">
        <thead>
          <tr>
            <th>Version</th>
            <th>Updated</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {history.map(h => (
            <React.Fragment key={h.version_number}>
              <tr
                className={`history-row ${expandedVersion === h.version_number ? 'selected' : ''}`}
              >
                <td>v{h.version_number}</td>
                <td>{new Date(h.updated_at).toLocaleString()}</td>
                <td>
                  <button
                    className="audit-btn"
                    onClick={() => setExpandedVersion(
                      expandedVersion === h.version_number ? null : h.version_number
                    )}
                  >
                    {expandedVersion === h.version_number ? 'Hide' : 'View'}
                  </button>
                </td>
              </tr>
              {expandedVersion === h.version_number && (
                <tr className="history-text-row">
                  <td colSpan="3">
                    <pre className="prompt-text">{h.prompt_text}</pre>
                  </td>
                </tr>
              )}
            </React.Fragment>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function PromptEditor({ prompt, onSave }) {
  const [editText, setEditText] = useState(prompt.prompt_text)
  const [saving, setSaving] = useState(false)
  const [editing, setEditing] = useState(false)
  const [showHistory, setShowHistory] = useState(false)

  const isDirty = editText !== prompt.prompt_text

  const handleSave = async () => {
    setSaving(true)
    try {
      const res = await fetch(`/api/prompts/${prompt.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt_text: editText }),
      })
      if (res.ok) {
        const data = await res.json()
        onSave({ ...prompt, prompt_text: editText, version_number: data.version_number })
        setEditing(false)
        setShowHistory(false)
      }
    } finally {
      setSaving(false)
    }
  }

  const handleCancel = () => {
    setEditText(prompt.prompt_text)
    setEditing(false)
  }

  return (
    <div className="prompt-editor">
      {editing ? (
        <>
          <textarea
            className="prompt-textarea"
            value={editText}
            onChange={(e) => setEditText(e.target.value)}
            rows={12}
          />
          <div className="prompt-actions">
            <button className="save-btn" onClick={handleSave} disabled={!isDirty || saving}>
              {saving ? 'Saving...' : 'Save'}
            </button>
            <button className="cancel-btn" onClick={handleCancel}>Cancel</button>
          </div>
        </>
      ) : (
        <>
          <pre className="prompt-text">{prompt.prompt_text}</pre>
          <div className="prompt-actions">
            <button className="edit-btn" onClick={() => setEditing(true)}>Edit</button>
            {prompt.version_number > 1 && (
              <button
                className={`edit-btn ${showHistory ? 'active' : ''}`}
                onClick={() => setShowHistory(!showHistory)}
              >
                {showHistory ? 'Hide History' : 'History'}
              </button>
            )}
          </div>
        </>
      )}
      {showHistory && <PromptHistoryPanel promptId={prompt.id} />}
    </div>
  )
}

function PromptsTab() {
  const [prompts, setPrompts] = useState([])
  const [loading, setLoading] = useState(true)
  const [expandedId, setExpandedId] = useState(null)

  useEffect(() => {
    fetch('/api/prompts')
      .then(res => res.json())
      .then(data => {
        setPrompts(data)
        setLoading(false)
      })
  }, [])

  const handlePromptSave = (updated) => {
    setPrompts(prompts.map(p => p.id === updated.id ? updated : p))
  }

  if (loading) return <div className="loading">Loading prompts...</div>

  const agents = [...new Set(prompts.map(p => p.agent_name))]

  return (
    <>
      <p className="subtitle">{prompts.length} prompts across {agents.length} agents</p>
      {agents.map(agentName => (
        <div key={agentName} className="agent-section">
          <h3 className="agent-name">{agentName}</h3>
          <table className="prompts-table">
            <thead>
              <tr>
                <th>Prompt Key</th>
                <th>Type</th>
                <th>Version</th>
                <th>Description</th>
                <th>Active</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {prompts.filter(p => p.agent_name === agentName).map(p => (
                <React.Fragment key={p.id}>
                  <tr className={`prompt-row ${expandedId === p.id ? 'selected' : ''}`}>
                    <td className="id-cell">{p.prompt_key}</td>
                    <td><span className={`type-badge type-${p.prompt_type}`}>{p.prompt_type}</span></td>
                    <td>v{p.version_number}</td>
                    <td>{p.description || '—'}</td>
                    <td>{p.is_active ? 'Yes' : 'No'}</td>
                    <td>
                      <button
                        className="audit-btn"
                        onClick={() => setExpandedId(expandedId === p.id ? null : p.id)}
                      >
                        {expandedId === p.id ? 'Hide' : 'View'}
                      </button>
                    </td>
                  </tr>
                  {expandedId === p.id && (
                    <tr className="prompt-text-row">
                      <td colSpan="6">
                        <PromptEditor prompt={p} onSave={handlePromptSave} />
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </>
  )
}

function App() {
  const [activeTab, setActiveTab] = useState('workflows')

  return (
    <div className="app">
      <h1>TrialAgent Dashboard</h1>
      <div className="tabs">
        <button
          className={`tab ${activeTab === 'workflows' ? 'active' : ''}`}
          onClick={() => setActiveTab('workflows')}
        >
          Workflows
        </button>
        <button
          className={`tab ${activeTab === 'prompts' ? 'active' : ''}`}
          onClick={() => setActiveTab('prompts')}
        >
          Prompts
        </button>
      </div>
      {activeTab === 'workflows' && <WorkflowsTab />}
      {activeTab === 'prompts' && <PromptsTab />}
    </div>
  )
}

export default App

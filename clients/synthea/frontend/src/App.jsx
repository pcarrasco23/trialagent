import React, { useState, useEffect } from 'react'
import './App.css'

function PatientWorkflowsPanel({ patientId }) {
  const [loading, setLoading] = useState(true)
  const [workflows, setWorkflows] = useState([])
  const [selectedWorkflow, setSelectedWorkflow] = useState(null)
  const [expandedTrialId, setExpandedTrialId] = useState(null)
  const [selectedModel, setSelectedModel] = useState('gpt-4.1')
  const [selectedCorpus, setSelectedCorpus] = useState('clinical_trials_gov')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState(null)
  const [workflowId, setWorkflowId] = useState(null)
  const [steps, setSteps] = useState([])
  const [rankings, setRankings] = useState([])

  const addStep = (message, displayType = 'status') => {
    setSteps(prev => {
      if (prev.length > 0 && prev[prev.length - 1].message === message) return prev
      const updated = prev.map(s => ({ ...s, active: false }))
      return [...updated, { message, active: displayType === 'status', displayType }]
    })
  }

  const deactivateSteps = () => {
    setSteps(prev => prev.map(s => ({ ...s, active: false })))
  }

  const loadWorkflows = () => {
    setLoading(true)
    fetch(`/api/patients/${patientId}/ranking_results`)
      .then(res => res.ok ? res.json() : [])
      .then(data => { setWorkflows(data); setLoading(false) })
      .catch(() => { setWorkflows([]); setLoading(false) })
  }

  useEffect(() => { loadWorkflows() }, [patientId])

  const runWorkflow = async () => {
    setSubmitting(true)
    setError(null)
    setSteps([])
    setRankings([])
    setExpandedTrialId(null)
    try {
      const res = await fetch(`/api/patients/${patientId}/run-workflow`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ model: selectedModel, trial_corpus: selectedCorpus }),
      })
      if (!res.ok) throw new Error(`Failed (${res.status})`)
      const data = await res.json()
      setWorkflowId(data.workflow_id)
    } catch (err) {
      setError(err.message)
    } finally {
      setSubmitting(false)
    }
  }

  useEffect(() => {
    if (!workflowId) return
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/workflow/${workflowId}`)
    ws.onopen = async () => {
      const res = await fetch(`/api/workflows/${workflowId}/status`)
      if (res.ok) {
        const data = await res.json()
        if (data.agent_message) addStep(data.agent_message)
        if (data.status === 'completed') {
          setSteps(prev => [...prev.map(s => ({ ...s, active: false })), { message: 'Completed', active: false }])
          const rankRes = await fetch(`/api/workflows/${workflowId}/ranking_results`)
          if (rankRes.ok) { const d = await rankRes.json(); setRankings(d.ranking_results || d) }
          loadWorkflows()
          ws.close()
        } else if (data.status === 'failed') {
          deactivateSteps()
          ws.close()
        }
      }
    }
    ws.onmessage = async (event) => {
      const data = JSON.parse(event.data)
      if (data.agent_message !== undefined && data.agent_message) {
        addStep(data.agent_message, data.display_type || 'status')
      }
      if (data.status) {
        if (data.status === 'completed') {
          setSteps(prev => [...prev.map(s => ({ ...s, active: false })), { message: 'Completed', active: false }])
          const rankRes = await fetch(`/api/workflows/${workflowId}/ranking_results`)
          if (rankRes.ok) { const d = await rankRes.json(); setRankings(d.ranking_results || d) }
          loadWorkflows()
          ws.close()
        } else if (data.status === 'failed') {
          deactivateSteps()
          ws.close()
        }
      }
    }
    ws.onerror = () => ws.close()
    return () => ws.close()
  }, [workflowId])

  return (
    <div className="workflow-panel">
      <div className="config-panel">
        <div className="config-row">
          <label>Model</label>
          <select value={selectedModel} onChange={e => setSelectedModel(e.target.value)}>
            <option value="gpt-4">gpt-4</option>
            <option value="gpt-4-turbo">gpt-4-turbo</option>
            <option value="gpt-4o">gpt-4o</option>
            <option value="gpt-4o-mini">gpt-4o-mini</option>
            <option value="gpt-4.1">gpt-4.1</option>
            <option value="gpt-4.1-mini">gpt-4.1-mini</option>
            <option value="gpt-5.1">gpt-5.1</option>
            <option value="meditron">meditron</option>
          </select>
        </div>
        <div className="config-row">
          <label>Trial Corpus</label>
          <select value={selectedCorpus} onChange={e => setSelectedCorpus(e.target.value)}>
            <option value="clinical_trials_gov">clinical_trials_gov</option>
            <option value="trec_2021_trial_corpus">trec_2021_trial_corpus</option>
          </select>
        </div>
        <div className="config-row">
          <label>
            <input type="checkbox" disabled checked={false} />
            {' '}Include QRELs
          </label>
        </div>
        <button className="workflow-btn" onClick={runWorkflow} disabled={submitting}>
          {submitting ? 'Submitting...' : 'Submit'}
        </button>
      </div>
      {error && <div className="failure-msg">{error}</div>}

      {steps.length > 0 && (
        <div className="steps-timeline">
          {steps.map((step, i) => (
            <div key={i} className={`step ${step.active ? 'active' : 'done'} ${i === steps.length - 1 ? 'last' : ''} ${step.displayType === 'result' ? 'result' : ''}`}>
              <div className="step-dot" />
              {step.displayType === 'result'
                ? <pre className="step-label result-pre">{step.message}</pre>
                : <span className="step-label">{step.message}</span>
              }
            </div>
          ))}
        </div>
      )}

      {rankings.length > 0 && (
        <table className="ranking-table">
          <thead>
            <tr>
              <th>Rank</th>
              <th>NCT ID</th>
              <th>Title</th>
              <th>Combined</th>
              <th>Relevance</th>
              <th>Eligibility</th>
            </tr>
          </thead>
          <tbody>
            {rankings.map((r, i) => (
              <React.Fragment key={r.nct_id || i}>
                <tr>
                  <td>{r.rank}</td>
                  <td>{r.nct_id}</td>
                  <td>{r.brief_title}</td>
                  <td>{r.combined_score?.toFixed(2)}</td>
                  <td>{r.relevance_score?.toFixed(2)}</td>
                  <td>
                    {r.eligibility_score?.toFixed(2)}
                    {(r.inclusion?.eligibility?.length > 0 || r.exclusion?.eligibility?.length > 0) && (
                      <a className="detail-link" onClick={() => setExpandedTrialId(expandedTrialId === r.nct_id ? null : r.nct_id)}>
                        {expandedTrialId === r.nct_id ? 'Hide Details' : 'More Details'}
                      </a>
                    )}
                  </td>
                </tr>
                {expandedTrialId === r.nct_id && ['inclusion', 'exclusion'].map(etype => (
                  r[etype]?.eligibility?.length > 0 && (
                    <tr key={etype} className="eligibility-detail-row">
                      <td colSpan="2"><strong>{etype === 'inclusion' ? 'Inclusion' : 'Exclusion'}</strong></td>
                      <td colSpan="2"><pre className="criteria-pre">{r[etype]?.criteria || '—'}</pre></td>
                      <td colSpan="2">
                        <table className="elig-sub-table">
                          <thead>
                            <tr><th>#</th><th>Reasoning</th><th>Label</th></tr>
                          </thead>
                          <tbody>
                            {r[etype].eligibility.map(e => (
                              <tr key={e.criterion_number}>
                                <td>{e.criterion_number}</td>
                                <td>{e.reasoning}</td>
                                <td><span className={`elig-label elig-${e.eligibility_label?.replace(/\s+/g, '-')}`}>{e.eligibility_label}</span></td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </td>
                    </tr>
                  )
                ))}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      )}

      {loading ? (
        <div className="loading">Loading workflows...</div>
      ) : workflows.length === 0 ? (
        <div className="no-conditions">No workflows found</div>
      ) : (
        <table className="workflows-table">
          <thead>
            <tr>
              <th>Workflow ID</th>
              <th>Status</th>
              <th>Model</th>
              <th>Trial Corpus</th>
            </tr>
          </thead>
          <tbody>
            {workflows.map(w => (
              <React.Fragment key={w.workflow_id}>
                <tr className={`workflow-history-row ${selectedWorkflow === w.workflow_id ? 'selected' : ''}`} onClick={() => { setSelectedWorkflow(selectedWorkflow === w.workflow_id ? null : w.workflow_id); setExpandedTrialId(null) }}>
                  <td>{w.workflow_id.slice(0, 8)}...</td>
                  <td>{w.status?.status}</td>
                  <td>{w.model}</td>
                  <td>{w.trial_corpus}</td>
                </tr>
                {selectedWorkflow === w.workflow_id && (
                  <tr className="workflow-detail-row">
                    <td colSpan="4">
                      {w.ranking_results && w.ranking_results.length > 0 ? (
                        <table className="ranking-table">
                          <thead>
                            <tr>
                              <th>Rank</th>
                              <th>NCT ID</th>
                              <th>Title</th>
                              <th>Combined</th>
                              <th>Relevance</th>
                              <th>Eligibility</th>
                            </tr>
                          </thead>
                          <tbody>
                            {w.ranking_results.map((r, i) => (
                              <React.Fragment key={r.nct_id || i}>
                                <tr>
                                  <td>{r.rank}</td>
                                  <td>{r.nct_id}</td>
                                  <td>{r.brief_title}</td>
                                  <td>{r.combined_score?.toFixed(2)}</td>
                                  <td>{r.relevance_score?.toFixed(2)}</td>
                                  <td>
                                    {r.eligibility_score?.toFixed(2)}
                                    {(r.inclusion?.eligibility?.length > 0 || r.exclusion?.eligibility?.length > 0) && (
                                      <a className="detail-link" onClick={(e) => { e.stopPropagation(); setExpandedTrialId(expandedTrialId === r.nct_id ? null : r.nct_id) }}>
                                        {expandedTrialId === r.nct_id ? 'Hide Details' : 'More Details'}
                                      </a>
                                    )}
                                  </td>
                                </tr>
                                {expandedTrialId === r.nct_id && ['inclusion', 'exclusion'].map(etype => (
                                  r[etype]?.eligibility?.length > 0 && (
                                    <tr key={etype} className="eligibility-detail-row">
                                      <td colSpan="2"><strong>{etype === 'inclusion' ? 'Inclusion' : 'Exclusion'}</strong></td>
                                      <td colSpan="2"><pre className="criteria-pre">{r[etype]?.criteria || '—'}</pre></td>
                                      <td colSpan="2">
                                        <table className="elig-sub-table">
                                          <thead><tr><th>#</th><th>Reasoning</th><th>Label</th></tr></thead>
                                          <tbody>
                                            {r[etype].eligibility.map(e => (
                                              <tr key={e.criterion_number}>
                                                <td>{e.criterion_number}</td>
                                                <td>{e.reasoning}</td>
                                                <td><span className={`elig-label elig-${e.eligibility_label?.replace(/\s+/g, '-')}`}>{e.eligibility_label}</span></td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      </td>
                                    </tr>
                                  )
                                ))}
                              </React.Fragment>
                            ))}
                          </tbody>
                        </table>
                      ) : (
                        <div className="no-conditions">No ranking results for this workflow</div>
                      )}
                    </td>
                  </tr>
                )}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

function App() {
  const [patients, setPatients] = useState([])
  const [expandedId, setExpandedId] = useState(null)
  const [expandedPanel, setExpandedPanel] = useState(null)
  const [conditions, setConditions] = useState([])
  const [observations, setObservations] = useState([])
  const [loading, setLoading] = useState(true)
  const [detailLoading, setDetailLoading] = useState(false)

  useEffect(() => {
    fetch('/api/patients')
      .then(res => res.json())
      .then(data => {
        setPatients(data)
        setLoading(false)
      })
  }, [])

  const togglePanel = async (patientId, panel) => {
    if (expandedId === patientId && expandedPanel === panel) {
      setExpandedId(null)
      setExpandedPanel(null)
      return
    }
    setExpandedId(patientId)
    setExpandedPanel(panel)

    if (panel === 'conditions') {
      setDetailLoading(true)
      try {
        const res = await fetch(`/api/patients/${patientId}/conditions`)
        setConditions(res.ok ? await res.json() : [])
      } catch { setConditions([]) }
      setDetailLoading(false)
    } else if (panel === 'observations') {
      setDetailLoading(true)
      try {
        const res = await fetch(`/api/patients/${patientId}/observations`)
        setObservations(res.ok ? await res.json() : [])
      } catch { setObservations([]) }
      setDetailLoading(false)
    }
  }

  if (loading) return <div className="loading">Loading patients...</div>

  return (
    <div className="app">
      <h1>Synthea Patient Viewer</h1>
      <p className="subtitle">{patients.length} patients</p>
      <table className="patient-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Gender</th>
            <th>Date of Birth</th>
            <th>City</th>
            <th>State</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {patients.map(patient => (
            <React.Fragment key={patient.id}>
              <tr className="patient-row">
                <td>{patient.given_names} {patient.family_name}</td>
                <td>{patient.gender}</td>
                <td>{patient.birth_date}</td>
                <td>{patient.city}</td>
                <td>{patient.state}</td>
                <td className="workflow-cell">
                  <button
                    className={`panel-btn ${expandedId === patient.id && expandedPanel === 'conditions' ? 'active' : ''}`}
                    onClick={() => togglePanel(patient.id, 'conditions')}
                  >
                    Conditions
                  </button>
                  <button
                    className={`panel-btn ${expandedId === patient.id && expandedPanel === 'observations' ? 'active' : ''}`}
                    onClick={() => togglePanel(patient.id, 'observations')}
                  >
                    Diagnostic Reports
                  </button>
                  <button
                    className={`panel-btn ${expandedId === patient.id && expandedPanel === 'workflows' ? 'active' : ''}`}
                    onClick={() => togglePanel(patient.id, 'workflows')}
                  >
                    Workflow
                  </button>
                </td>
              </tr>
              {expandedId === patient.id && expandedPanel === 'workflows' && (
                <tr className="conditions-row">
                  <td colSpan="6">
                    <PatientWorkflowsPanel patientId={patient.id} />
                  </td>
                </tr>
              )}
              {expandedId === patient.id && expandedPanel === 'conditions' && (
                <tr className="conditions-row">
                  <td colSpan="6">
                    {detailLoading ? (
                      <div className="loading">Loading conditions...</div>
                    ) : conditions.length === 0 ? (
                      <div className="no-conditions">No conditions recorded</div>
                    ) : (
                      <table className="conditions-table">
                        <thead>
                          <tr>
                            <th>Condition</th>
                            <th>Code</th>
                            <th>Status</th>
                            <th>Category</th>
                            <th>Onset</th>
                          </tr>
                        </thead>
                        <tbody>
                          {conditions.map(c => (
                            <tr key={c.id}>
                              <td>{c.display || c.code}</td>
                              <td>{c.code}</td>
                              <td>{c.clinical_status}</td>
                              <td>{c.category}</td>
                              <td>{c.onset_datetime?.slice(0, 10)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </td>
                </tr>
              )}
              {expandedId === patient.id && expandedPanel === 'observations' && (
                <tr className="conditions-row">
                  <td colSpan="6">
                    {detailLoading ? (
                      <div className="loading">Loading diagnostic reports...</div>
                    ) : observations.length === 0 ? (
                      <div className="no-conditions">No diagnostic reports found</div>
                    ) : (
                      <table className="conditions-table">
                        <thead>
                          <tr>
                            <th>Report</th>
                            <th>Observation</th>
                            <th>Code</th>
                            <th>Value</th>
                            <th>Date</th>
                          </tr>
                        </thead>
                        <tbody>
                          {observations.map((o, i) => (
                            <tr key={i}>
                              <td>{o.report_display || '—'}</td>
                              <td>{o.obs_display || '—'}</td>
                              <td>{o.obs_code || o.report_code}</td>
                              <td>
                                {o.value_quantity != null
                                  ? `${Number(o.value_quantity).toFixed(2)} ${o.value_unit || ''}`
                                  : o.value_string || '—'}
                              </td>
                              <td>{o.effective_date?.slice(0, 10) || '—'}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
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

export default App

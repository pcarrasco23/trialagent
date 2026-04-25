import React, { useState, useEffect } from 'react'
import './App.css'

function PatientWorkflowsPanel({ topicNumber, patientId }) {
  const [loading, setLoading] = useState(true)
  const [workflows, setWorkflows] = useState([])
  const [selectedWorkflow, setSelectedWorkflow] = useState(null)
  const [expandedTrialId, setExpandedTrialId] = useState(null)
  const [selectedModel, setSelectedModel] = useState('gpt-4.1')
  const [selectedCorpus, setSelectedCorpus] = useState('trec_2021_trial_corpus')
  const [includeQrels, setIncludeQrels] = useState(true)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState(null)
  const [workflowId, setWorkflowId] = useState(null)
  const [steps, setSteps] = useState([])
  const [rankings, setRankings] = useState([])
  const [qrelsResults, setQrelsResults] = useState(null)

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
    setQrelsResults(null)
    setExpandedTrialId(null)
    try {
      const res = await fetch(`/api/topics/${topicNumber}/run-workflow`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          model: selectedModel,
          trial_corpus: selectedCorpus,
          include_qrels: includeQrels,
        }),
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
          if (rankRes.ok) { const d = await rankRes.json(); setRankings(d.ranking_results || d); setQrelsResults(d.qrels_results || null) }
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
          if (rankRes.ok) { const d = await rankRes.json(); setRankings(d.ranking_results || d); setQrelsResults(d.qrels_results || null) }
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
    <div className="workflow-panel mt-2">
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
            <input type="checkbox" checked={includeQrels} onChange={e => setIncludeQrels(e.target.checked)} />
            {' '}Include QRELs
          </label>
        </div>
        <button className="btn btn-primary btn-sm" onClick={runWorkflow} disabled={submitting}>
          {submitting ? 'Submitting...' : 'Submit'}
        </button>
      </div>
      {error && <span className="text-danger">{error}</span>}

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

      {qrelsResults && (
        <table className="table table-sm table-bordered mt-2 mb-1" style={{width: 'auto'}}>
          <thead className="table-info">
            <tr><th>Metric</th><th>Value</th></tr>
          </thead>
          <tbody>
            {Object.entries(qrelsResults).sort(([a],[b]) => a.localeCompare(b)).map(([name, value]) => (
              <tr key={name}><td>{name}</td><td>{value.toFixed(4)}</td></tr>
            ))}
          </tbody>
        </table>
      )}

      {rankings.length > 0 && (
        <table className="table table-bordered mt-2 mb-0">
          <thead className="table-success">
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
                  <td><code>{r.nct_id}</code></td>
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

      {loading ? <div className="text-muted">Loading...</div> : (
        workflows.length === 0 ? <div className="text-muted">No workflows found</div> : (
          <table className="table table-sm mt-2">
            <thead>
              <tr><th>Workflow ID</th><th>Status</th><th>Model</th><th>Trial Corpus</th></tr>
            </thead>
            <tbody>
              {workflows.map(w => (
                <React.Fragment key={w.workflow_id}>
                  <tr onClick={() => { setSelectedWorkflow(selectedWorkflow === w.workflow_id ? null : w.workflow_id); setExpandedTrialId(null) }} style={{cursor: 'pointer'}}>
                    <td>{w.workflow_id}</td>
                    <td>{w.status?.status}</td>
                    <td>{w.model}</td>
                    <td>{w.trial_corpus}</td>
                  </tr>
                  {selectedWorkflow === w.workflow_id && (
                    <tr>
                      <td colSpan="4">
                        {w.qrels_results && (
                          <table className="table table-sm table-bordered mb-1" style={{width: 'auto'}}>
                            <thead className="table-info">
                              <tr><th>Metric</th><th>Value</th></tr>
                            </thead>
                            <tbody>
                              {Object.entries(w.qrels_results).sort(([a],[b]) => a.localeCompare(b)).map(([name, value]) => (
                                <tr key={name}><td>{name}</td><td>{value.toFixed(4)}</td></tr>
                              ))}
                            </tbody>
                          </table>
                        )}
                        {w.ranking_results && w.ranking_results.length > 0 ? (
                          <table className="table table-bordered mb-0">
                            <thead className="table-success">
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
                                    <td><code>{r.nct_id}</code></td>
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
                        ) : <div className="text-muted">No ranking results</div>}
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        )
      )}
    </div>
  )
}

function App() {
  const [topics, setTopics] = useState([])
  const [detailNumber, setDetailNumber] = useState(null)
  const [workflowsNumber, setWorkflowsNumber] = useState(null)
  const [topicDetail, setTopicDetail] = useState(null)
  const [loading, setLoading] = useState(true)
  const [detailLoading, setDetailLoading] = useState(false)

  useEffect(() => {
    fetch('/api/topics')
      .then(res => res.json())
      .then(data => {
        setTopics(data)
        setLoading(false)
      })
  }, [])

  const toggleDetail = async (number) => {
    if (detailNumber === number) {
      setDetailNumber(null)
      return
    }
    setDetailNumber(number)
    setDetailLoading(true)
    try {
      const res = await fetch(`/api/topics/${number}`)
      if (res.ok) setTopicDetail(await res.json())
    } catch { setTopicDetail(null) }
    setDetailLoading(false)
  }

  const toggleWorkflows = (number) => {
    setWorkflowsNumber(workflowsNumber === number ? null : number)
  }

  if (loading) {
    return (
      <div className="container mt-4 text-center">
        <div className="spinner-border text-primary" role="status"></div>
        <p className="text-muted mt-2">Loading topics...</p>
      </div>
    )
  }

  return (
    <div className="container mt-4">
      <h2 className="mb-1">TREC 2021 Clinical Trials</h2>
      <p className="text-muted mb-3">{topics.length} topics</p>
      <div className="card">
        <table className="table table-hover mb-0">
          <thead className="table-light">
            <tr>
              <th>Topic</th>
              <th>Patient Summary</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {topics.map(topic => (
              <React.Fragment key={topic.number}>
                <tr>
                  <td>{topic.number}</td>
                  <td>{topic.preview}...</td>
                  <td className="btn-cell">
                    <button
                      className={`btn btn-outline-primary btn-sm ${detailNumber === topic.number ? 'active' : ''}`}
                      onClick={() => toggleDetail(topic.number)}
                    >
                      View
                    </button>
                    <button
                      className={`btn btn-outline-secondary btn-sm ${workflowsNumber === topic.number ? 'active' : ''}`}
                      onClick={() => toggleWorkflows(topic.number)}
                    >
                      Workflow
                    </button>
                  </td>
                </tr>
                {detailNumber === topic.number && (
                  <tr>
                    <td colSpan="3" className="bg-light p-3">
                      {detailLoading ? (
                        <div className="text-center text-muted py-3">
                          <div className="spinner-border spinner-border-sm text-primary me-2" role="status"></div>
                          Loading...
                        </div>
                      ) : topicDetail ? (
                        <pre className="topic-text">{topicDetail.text}</pre>
                      ) : (
                        <p className="text-muted fst-italic mb-0">Failed to load topic</p>
                      )}
                    </td>
                  </tr>
                )}
                {workflowsNumber === topic.number && (
                  <tr>
                    <td colSpan="3" className="bg-light p-3">
                      <PatientWorkflowsPanel topicNumber={topic.number} patientId={`trec-2021-${topic.number}`} />
                    </td>
                  </tr>
                )}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default App

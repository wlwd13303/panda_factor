import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from '@/components/Layout'
import DataClean from '@/pages/DataClean'
import FactorAnalysis from '@/pages/FactorAnalysis'
import FactorAnalysisWithAI from '@/pages/FactorAnalysisWithAI'
import FactorList from '@/pages/FactorList'
import FactorEditor from '@/pages/FactorEditor'
import LLMChat from '@/pages/LLMChat'

const AppRoutes = () => {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<Navigate to="/factor-list" replace />} />
        <Route path="data-clean" element={<DataClean />} />
        <Route path="factor-list" element={<FactorList />} />
        <Route path="factor-editor/:id" element={<FactorEditor />} />
        <Route path="factor-analysis-ai" element={<FactorAnalysisWithAI />} />
        <Route path="factor-analysis" element={<FactorAnalysis />} />
        <Route path="llm-chat" element={<LLMChat />} />
      </Route>
    </Routes>
  )
}

export default AppRoutes


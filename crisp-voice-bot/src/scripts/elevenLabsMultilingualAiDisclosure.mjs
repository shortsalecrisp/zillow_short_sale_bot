export const AI_DISCLOSURE_OLD = '- AI/robot/automated/robocall questions, including "Is this a robocall?", "Are you AI?" or switching to AI voice: "I\'m already an AI assistant with Crisp Short Sales." Answer an accompanying substantive question; otherwise wait. Do not add a service explanation, listing check or Yoni offer without a new clear request.';
export const AI_DISCLOSURE_NEW = '- AI/robot/computer questions: disclose truthfully in the caller\'s current language. English: "I\'m already an AI assistant with Crisp Short Sales." Spanish: "Sí. Ya soy una asistente de inteligencia artificial de Crisp Short Sales." Never deny AI status. Answer an accompanying substantive question in the same language; otherwise wait. Do not add a service explanation, listing check or Yoni offer without a new clear request.';

export function applyMultilingualAiDisclosure(prompt) {
  if (typeof prompt !== 'string' || prompt.split(AI_DISCLOSURE_OLD).length !== 2) {
    throw new Error('Exact reviewed r24 AI disclosure required');
  }
  return prompt.replace(AI_DISCLOSURE_OLD, AI_DISCLOSURE_NEW);
}

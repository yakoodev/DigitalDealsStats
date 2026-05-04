/**
 * Нормализует точки истории для графиков: удаляет невалидные значения
 * и сортирует по дате по возрастанию.
 * @param {Array<{generated_at?: string, matched_offers?: number}>} points
 * @returns {Array<{generated_at: string, matched_offers: number}>}
 */
export function normalizeHistoryPoints(points) {
  return (Array.isArray(points) ? points : [])
    .filter((item) => item && typeof item.generated_at === "string")
    .map((item) => ({
      generated_at: item.generated_at,
      matched_offers: Number.isFinite(Number(item.matched_offers))
        ? Number(item.matched_offers)
        : 0,
    }))
    .sort((left, right) => new Date(left.generated_at).getTime() - new Date(right.generated_at).getTime());
}

export function Metric({
  label,
  value,
  detail,
  accent = "",
}: {
  label: string;
  value: number;
  detail: string;
  accent?: string;
}) {
  return (
    <div className={`ct-metric ct-metric--${accent}`}>
      <span>{label}</span>
      <div>
        <strong>{value.toString().padStart(2, "0")}</strong>
      </div>
      <small>{detail}</small>
    </div>
  );
}

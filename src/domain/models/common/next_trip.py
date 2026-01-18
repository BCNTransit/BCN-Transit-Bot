from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

SPAIN_TZ = ZoneInfo("Europe/Madrid")

@dataclass
class NextTrip:
    id: str
    arrival_time: int # Epoch in seconds
    delay_in_minutes: int = 0
    platform: str = ""

    def remaining_time(self, arriving_threshold=40) -> str:
        if not self.arrival_time:
            return "-"
    
        now_ts = datetime.now(SPAIN_TZ).timestamp()
        delta_s = self.arrival_time - now_ts

        if delta_s <= arriving_threshold:
            return "🔜"

        hours, remainder = divmod(int(delta_s), 3600)
        minutes, seconds = divmod(remainder, 60)

        parts = []
        if hours:
            parts.append(f"{hours}h")
        if minutes:
            parts.append(f"{minutes}min")
        if seconds or not minutes:
            parts.append(f"{seconds}s")

        return " ".join(parts)

    def remaining_time_and_arrival_date(self, arriving_threshold = 40) -> str:
        if not self.arrival_time:
            return "-"
    
        remaining_time = self.remaining_time(arriving_threshold)

        arrival_dt = datetime.fromtimestamp(self.arrival_time)

        if arrival_dt.date() == datetime.now().date():
            return f"{remaining_time} → {arrival_dt.strftime('⏰ %H:%Mh')}"

        return arrival_dt.strftime("%d-%m-%Y → ⏰ %H:%Mh")
    
    def scheduled_arrival(self) -> datetime:
        """Devuelve la hora programada de llegada en base al retraso."""
        if not self.arrival_time:
            return None
        return datetime.fromtimestamp(self.arrival_time) - timedelta(minutes=self.delay_in_minutes or 0)
    
def normalize_to_seconds(ts: int) -> int:
    # Si parece milisegundos, lo pasamos a segundos
    return ts // 1000 if ts > 1e11 else ts
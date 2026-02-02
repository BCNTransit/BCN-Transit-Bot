from dataclasses import dataclass
from datetime import datetime
from typing import List
import html

from src.domain.enums.transport_type import TransportType

@dataclass
class Publication:
    headerCa: str
    headerEn: str
    headerEs: str
    textCa: str
    textEn: str
    textEs: str

@dataclass
class AffectedEntity:
    direction_code: str
    direction_name: str
    entrance_code: str
    entrance_name: str
    line_code: str
    line_name: str
    station_code: str
    station_name: str

@dataclass
class Alert:
    id: str
    transport_type: TransportType
    begin_date: datetime
    end_date: datetime
    status: str
    cause: str
    publications: List[Publication]
    affected_entities: List[AffectedEntity]

    def _get_alert_content(self):
        # 1. Título y Descripción
        title = self.publications[0].headerEs if self.publications and self.publications[0].headerEs else "Incidencia"
        description = html.unescape(self.publications[0].textEs) if self.publications and self.publications[0].textEs else ""

        # 2. Lógica de Líneas (Colapso inteligente)
        lineas = sorted({e.line_code for e in self.affected_entities if e.line_code})
        if not lineas:
            lineas_summary = "Aviso"
        elif len(lineas) > 3:
            lineas_summary = f"Global +{len(lineas)}" 
        else:
            lineas_summary = ", ".join(lineas)

        # 3. Lógica de Estaciones (NUEVO)
        estaciones = sorted({e.station_name for e in self.affected_entities if e.station_name})
        estaciones_summary = ""
        
        if len(estaciones) == 1:
            # Si solo hay una, la mostramos: "L1 [Catalunya]"
            estaciones_summary = f" [{estaciones[0]}]"
        elif len(estaciones) > 1:
            # Si hay varias, indicamos cuántas para no saturar
            estaciones_summary = f" ({len(estaciones)} estaciones)"
        # Si no hay estaciones (alerta de línea), se queda vacío

        # 4. Mapeo de Causas
        cause_map = {
            "TECHNICAL": "⚙️", "ACCIDENT": "🚑", "WORKS": "🚧", 
            "EVENT": "🎉", "STRIKE": "📢", "OTHER": "ℹ️"
        }
        emoji = cause_map.get(self.cause.upper() if self.cause else '', "🚨")

        # 5. CONSTRUCCIÓN DEL PUSH_BODY
        # Formato: 🚨 [L1] [Catalunya] Título...
        # O: 🚨 [L5] (4 estaciones) Título...
        push_body = f"{emoji} [{lineas_summary}]{estaciones_summary} {title}".strip()

        # Recorte final de seguridad
        if len(push_body) > 120:
            push_body = push_body[:117] + "..."

        return title, lineas_summary, estaciones_summary, description, push_body
    

    def format_html_alert(self):
        title, begin_str, end_str, lineas_str, estaciones_str, cause_str, description = self._get_alert_content()
        return (
            f"🚨 <b>NUEVA ALERTA</b> 🚨\n\n"
            f"<u>{title}:</u>\n\n"
            f"🕒 <b>Desde:</b> {begin_str}\n"
            f"⏳ <b>Hasta:</b> {end_str}\n\n"
            f"🚇 <b>Líneas:</b> {lineas_str}\n"
            f"📍 <b>Estaciones:</b> {estaciones_str}\n"
            f"❗ <b>Causa:</b> {cause_str}\n\n"
            f"📝 <b>Info:</b>\n<i>{description}</i>"
        )


    @staticmethod
    def map_from_metro_alert(metro_alert):
        from src.application.utils.html_helper import HtmlHelper 

        publications = []
        publications.extend(
            Publication(
                headerCa=metro_alert_publication.get('headerCa', None),
                headerEn=metro_alert_publication.get('headerEn', None),
                headerEs=metro_alert_publication.get('headerEs', None),
                textCa=HtmlHelper.clean_text(metro_alert_publication.get('textCa', '')),
                textEn=HtmlHelper.clean_text(metro_alert_publication.get('textEn', '')),
                textEs=HtmlHelper.clean_text(metro_alert_publication.get('textEs', '')),
            )
            for metro_alert_publication in metro_alert.get('publications')
        )
        affected_entities = []
        affected_entities.extend(
            AffectedEntity(
                direction_code=entity.get('direction_code'),
                direction_name=entity.get('direction_name'),
                entrance_code=entity.get('entrance_code'),
                entrance_name=entity.get('entrance_name'),
                line_code=entity.get('line_code'),
                line_name=entity.get('line_name'),
                station_code=entity.get('station_code'),
                station_name=entity.get('station_name'),
            )
            for entity in metro_alert.get('entities')
        )
        return Alert(
            id=str(metro_alert.get('id')),
            transport_type=TransportType.METRO,
            begin_date=datetime.fromtimestamp(metro_alert.get('disruption_dates')[0].get('begin_date', None) / 1000) if metro_alert.get('disruption_dates')[0].get('begin_date', None) else None,
            end_date=datetime.fromtimestamp(metro_alert.get('disruption_dates')[0].get('end_date', None) / 1000) if metro_alert.get('disruption_dates')[0].get('end_date', None) else None,
            publications=publications,
            affected_entities=affected_entities,
            status=metro_alert.get('effect').get('status'),
            cause=metro_alert.get('cause').get('code')
        )

    @staticmethod
    def map_from_bus_alert(bus_alert):
        from src.application.utils.html_helper import HtmlHelper

        channel_info = bus_alert.get('channelInfoTO')
        publications = [Publication(
                headerCa=HtmlHelper.clean_text(bus_alert.get('typeName', '')),
                headerEn=HtmlHelper.clean_text(bus_alert.get('typeName', '')),
                headerEs=HtmlHelper.clean_text(bus_alert.get('typeName', '')),
                textCa=HtmlHelper.clean_text(channel_info.get('textCa', '')),
                textEn=HtmlHelper.clean_text(channel_info.get('textEn', '')),
                textEs=HtmlHelper.clean_text(channel_info.get('textEs', '')),
            )]
        
        affected_entities = []
        for entity in bus_alert.get('linesAffected'):
            ways = entity.get('ways')
            for way in ways:
                affected_entities.extend(
                    AffectedEntity(
                        direction_code=way.get('wayId'),
                        direction_name=way.get('wayName'),
                        entrance_code=None,
                        entrance_name=None,
                        line_code=entity.get('lineId'),
                        line_name=entity.get('commercialLineId'),
                        station_code=stop.get('stopId'),
                        station_name=stop.get('stopName'),
                    )
                    for stop in way.get('stops')
                )
        return Alert(
            id=str(bus_alert.get('id')),
            transport_type=TransportType.BUS,
            begin_date=datetime.fromtimestamp(bus_alert.get('begin') / 1000),
            end_date=datetime.fromtimestamp(bus_alert.get('end') / 1000),
            publications=publications,
            affected_entities=affected_entities,
            status=HtmlHelper.clean_text(bus_alert.get('causeName')),
            cause=bus_alert.get('categories').get('messageType')
        )

    @staticmethod
    def map_from_rodalies_alert(rodalies_alert):
        from src.application.utils.html_helper import HtmlHelper

        title = rodalies_alert.get('title')
        description = rodalies_alert.get('description')
        publications = [
            Publication(
                headerCa=title.get('ca', None),
                headerEn=title.get('en', None),
                headerEs=title.get('es', None),
                textCa=HtmlHelper.clean_text(description.get('ca', '')),
                textEn=HtmlHelper.clean_text(description.get('en', '')),
                textEs=HtmlHelper.clean_text(description.get('es', '')),
            )
        ]
        affected_entities = []
        affected_entities.extend(
            AffectedEntity(
                direction_code=None,
                direction_name=None,
                entrance_code=None,
                entrance_name=None,
                line_code=entity.get('id'),
                line_name=entity.get('name'),
                station_code=None,
                station_name=None,
            )
            for entity in rodalies_alert.get('lines')
        )
        return Alert(
            id=str(rodalies_alert.get('externalId')),
            transport_type=TransportType.RODALIES,
            begin_date=datetime.fromisoformat(rodalies_alert.get('date')),
            end_date=None,
            publications=publications,
            affected_entities=affected_entities,
            status=None,
            cause=None
        )

    @staticmethod
    def map_from_tram_alert(tram_alert):
        from src.application.utils.html_helper import HtmlHelper

        alert_content = tram_alert.get('alert', {})
        title = alert_content.get('header_text').get('translation', {})
        description = alert_content.get('description_text').get('translation', {})
        publications = [
            Publication(
                headerCa=next((item["text"] for item in title if item["language"] == "cat"), None),
                headerEn=next((item["text"] for item in title if item["language"] == "en"), None),
                headerEs=next((item["text"] for item in title if item["language"] == "es"), None),
                textCa=HtmlHelper.clean_text(next((item["text"] for item in description if item["language"] == "cat"), '')),
                textEn=HtmlHelper.clean_text(next((item["text"] for item in description if item["language"] == "en"), '')),
                textEs=HtmlHelper.clean_text(next((item["text"] for item in description if item["language"] == "es"), '')),
            )
        ]
        affected_entities = []
        affected_entities.extend(
            AffectedEntity(
                direction_code=None,
                direction_name=None,
                entrance_code=None,
                entrance_name=None,
                line_code="T" + entity.get('route_id').split('_')[-1] if entity.get('route_id') else '',
                line_name="T" + entity.get('route_id').split('_')[-1] if entity.get('route_id') else '',
                station_code=None,
                station_name=None,
            )
            for entity in alert_content.get('informed_entity')
        )

        return Alert(
            id=str(tram_alert.get('id')),
            transport_type=TransportType.TRAM,
            begin_date=datetime.fromtimestamp(alert_content.get('active_period')[0]['start']) if alert_content.get('active_period') else None,
            end_date=None,
            publications=publications,
            affected_entities=affected_entities,
            status=None,
            cause=alert_content.get('effect', None)
        )
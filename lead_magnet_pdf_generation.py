from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, PageBreak, Image as Img, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.graphics.shapes import Drawing, Line
from pyairtable import Api,Table as airtable_obj
import os
import requests
import ast
import openai
import json
from industryInsight import get_cold_email_kpis
from data_collector import collect_information

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_CLIENT_TABLE_NAME = os.getenv("AIRTABLE_CLIENT_TABLE_NAME")
CLIENT_NAME = 'taippa_marketing'

pdfmetrics.registerFont(TTFont('Anton', 'Fonts/Anton-Regular.ttf'))
pdfmetrics.registerFont(TTFont('Poppins', 'Fonts/Poppins-Regular.ttf'))
pdfmetrics.registerFont(TTFont('PoppinsBold', 'Fonts/Poppins-Bold.ttf'))

# Getting dynamic images from client_details table specific to each client
def get_dynamic_images(table,record_id,attachment_column):
    record = table.get(record_id)
    attachments = record['fields'].get(attachment_column)
    file_names=[]
    if attachments:
        for attachment in attachments:
            image_url = attachment['url']
            file_name = attachment['filename']
            print("\n================")
            print(f"File name: {file_name}")
            response = requests.get(image_url)
            if response.status_code == 200:
                with open(file_name, "wb") as file:
                    file.write(response.content)
                print(f"Downloaded: {file_name}")
                file_names.append(file_name)
            else:
                print(f"Failed to download: {image_url}")
    else:
        print(f"No attachments found in column '{attachment_column}' for record ID '{record_id}'.")
    return file_names


def content_formatting_list(content, is_metrics=False):
    if is_metrics:
        # Parse metrics data which comes in format:
        # 1. **Open Rate:** 25.24%
        # 2. **Click-Through Rate (CTR):** 2-5%
        # 3. **Reply Rate:** 8.5%

        metrics_lines = content.strip().split('\n')
        metrics_data = []

        descriptions = {
            "Open Rate": "The percentage of recipients who open your email",
            "Click-Through Rate (CTR)": "The percentage of recipients who click on links in your email",
            "Reply Rate": "The percentage of recipients who reply to your email"
        }

        for line in metrics_lines:
            # Extract the metric name and value
            parts = line.split(':**')
            if len(parts) == 2:
                # Clean up the metric name
                metric_name = parts[0].replace('1.', '').replace('2.', '').replace('3.', '').replace('**', '').strip()
                # Clean up the value
                value = parts[1].strip()

                # Calculate Taippa target (higher percentages)
                try:
                    if metric_name == "Open Rate":
                        raw_value = float(value.replace('%', ''))
                        taippa_target = f"{raw_value * 1.1:.2f}%"  # 10% higher
                    elif metric_name == "Click-Through Rate (CTR)":
                        if '-' in value:
                            high_end = float(value.split('-')[1].replace('%', ''))
                            taippa_target = f"{high_end * 1.3:.1f}%"  # 30% higher than high end
                        else:
                            raw_value = float(value.replace('%', ''))
                            taippa_target = f"{raw_value * 1.3:.1f}%"  # 30% higher
                    else:  # Reply Rate
                        raw_value = float(value.replace('%', ''))
                        taippa_target = f"{raw_value * 1.5:.1f}%"  # 50% higher
                except ValueError:
                    print(f"Error processing value: {value} for {metric_name}")
                    taippa_target = "N/A"

                metrics_data.append({
                    "header": metric_name,
                    "description": descriptions.get(metric_name, ""),
                    "industry_average": value,
                    "taippa_target": taippa_target
                })

        return json.dumps(metrics_data)
    else:
        prompt = f"""Format the following content into ONLY a JSON array. Do not include any other text:
        [
            {{
                "header": "Emerging Trends in Email Marketing",
                "body": ""
            }},
            {{
                "header": "1. Personalization is Driving Higher Engagement",
                "body": "Emails with personalized subject lines are 26% more likely to be opened. Using AI tools we allow SMEs to personalize content dynamically, leveraging customer behavior and preferences."
            }}
        ]

        Content: {content}"""

        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": "You are a JSON formatter. Only output the exact JSON array requested, with no additional text, explanations, or formatting."
                },
                {"role": "user", "content": prompt}
            ]
        )

        result = response['choices'][0]['message']['content'].strip()

        try:
            # First try to parse as is
            json.loads(result)
            return result
        except:
            # If that fails, try to clean it up
            try:
                start = result.find('[')
                end = result.rfind(']') + 1
                if start != -1 and end != 0:
                    cleaned = result[start:end]
                    json.loads(cleaned)
                    return cleaned
                else:
                    raise ValueError("Could not find valid JSON array in response")
            except Exception as e:
                print(f"Error parsing response: {result}")
                raise e

def create_pdf_with_black_background(user_details,output_path, image_path):
    styles = getSampleStyleSheet()

    main_title = ParagraphStyle(
        "TitleStyle",
        parent=styles["Heading1"],
        fontName="Anton",
        fontSize=24,
        textColor=colors.HexColor("#c969f5"),
        alignment=1,
        spaceAfter=30,
        borderPadding=20,
        borderWidth=2,
        borderColor=colors.HexColor("#8A2BE2"),
        borderRadius=8
    )

    sub_heading = ParagraphStyle(
        "SubHeadingStyle",
        parent=styles["Heading2"],
        fontName="Anton",
        fontSize=18,
        textColor=colors.HexColor("#6292cc"),
        alignment=1,
        spaceAfter=15,
        spaceBefore=10
    )

    header_style = ParagraphStyle(
        "HeaderStyle",
        parent=styles["Heading2"],
        fontName="Anton",
        fontSize=14,
        textColor=colors.HexColor("#6292cc"),
        spaceBefore=10,
        spaceAfter=6,
        leftIndent=10,
        bulletIndent=5,
        leading=18
    )

    metrics_header_style = ParagraphStyle(
        "MetricsHeaderStyle",
        parent=styles["Heading2"],
        fontName="Anton",
        fontSize=16,
        textColor=colors.HexColor("#6292cc"),
        spaceBefore=15,
        spaceAfter=4
    )

    body_style = ParagraphStyle(
        "BodyStyle",
        parent=styles["BodyText"],
        fontName="Poppins",
        fontSize=10,
        textColor=colors.HexColor("#f6f2f7"),
        leading=14,
        spaceAfter=8,
        leftIndent=20,
        firstLineIndent=-10,
        alignment=0
    )

    metrics_style = ParagraphStyle(
        "MetricsStyle",
        parent=styles["BodyText"],
        fontName="Poppins",
        fontSize=12,
        textColor=colors.HexColor("#f6f2f7"),
        leading=16,
        spaceAfter=4,
        leftIndent=20
    )

    def draw_fancy_background(canvas, doc):
        canvas.setFillColor(colors.black)
        canvas.rect(0, 0, A4[0], A4[1], fill=1)

        if doc.page > 0:
            canvas.setFillColor(colors.HexColor("#6292cc"))
            canvas.setFont("Anton", 12)
            page_num = f"Page {doc.page}"
            canvas.drawString(A4[0] - 72, 30, page_num)
            canvas.setStrokeColor(colors.HexColor("#6292cc"))
            canvas.line(A4[0] - 82, 25, A4[0] - 40, 25)

    def create_divider():
        drawing = Drawing(A4[0] - 100, 20)
        line = Line(0, 10, A4[0] - 100, 10)
        line.strokeColor = colors.HexColor("#6292cc")
        line.strokeWidth = 1
        drawing.add(line)
        return drawing

    content = []
    api = Api(AIRTABLE_API_KEY)
    airtable = api.table(AIRTABLE_BASE_ID, AIRTABLE_CLIENT_TABLE_NAME)
    records = airtable.all(formula=f"{{client_id}} = '{CLIENT_NAME}'")
    company_lead_magnet = str(records[0]['fields']['company_lead_magnet_text'])

    try:
        result_new = content_formatting_list(company_lead_magnet, is_metrics=False)
        result_new = json.loads(result_new)
    except Exception as e:
        print(f"Error parsing content: {result_new}")
        raise e

    
    # if image_path and os.path.exists(image_path):
    #     img = Img(image_path)
    #     img.drawWidth = A4[0] - 2 * 0.6 * inch  # Full width minus margins
    #     img_ratio = 0.3  # This controls the height of the banner (0.3 = 30% of width)
    #     img.drawHeight = img.drawWidth * img_ratio
    #     content.append(img)

    content.append(Spacer(1, 0.3 * inch))
    content.append(Paragraph("How AI-Driven Lead Generation Can Revolutionize Your Business Growth?", main_title))
    content.append(create_divider())
    content.append(Spacer(1, 0.2 * inch))

    content.append(Paragraph("Emerging Trends in Email Marketing", sub_heading))
    content.append(Spacer(1, 0.2 * inch))

    all_points = []
    for point in result_new:
        if point['header'] != "Emerging Trends in Email Marketing":
            cell_content = [
                Paragraph(f"{point['header']}", header_style),
                Paragraph(point["body"], body_style)
            ]
            all_points.append(cell_content)

    if all_points:
        table_data = [
            [all_points[0], all_points[1]],
            [all_points[2], all_points[3]]
        ]
        col_widths = [(A4[0] - 1.4 * inch) / 2] * 2
        pdf_table = Table(table_data, colWidths=col_widths)
        pdf_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor("#1a1a1a")),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('PADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor("#2a2a2a")),
        ]))
        content.append(pdf_table)

    content.append(PageBreak())
    content.append(Paragraph("Standard industry benchmarks of your sector", main_title))

    try:
        industry = user_details.get('organization_industry','Real Estate')
        if industry in ['Unknown','']:
            industry = 'Real Estate'
        metrics = get_cold_email_kpis(industry)
        metrics_result = content_formatting_list(metrics, is_metrics=True)
        metrics_formatted = json.loads(metrics_result)

        for point in metrics_formatted:
            content.append(Paragraph(f"<b>{point['header']}</b>", metrics_header_style))
            content.append(Paragraph(point['description'], metrics_style))
            content.append(Paragraph(f"Industry Average: {point['industry_average']}", metrics_style))
            content.append(Paragraph(f"Taippa Target: {point['taippa_target']}", metrics_style))
            content.append(Spacer(1, 0.2 * inch))
    except Exception as e:
        print(f"Error processing metrics: {e}")
        raise
    
    content.append(Spacer(1, 0.2 * inch))
    content.append(Spacer(1, 0.2 * inch))
    content.append(Spacer(1, 0.2 * inch))
    content.append(Spacer(1, 0.2 * inch))
    content.append(Spacer(1, 0.2 * inch))
    content.append(Spacer(1, 0.2 * inch))
    content.append(Spacer(1, 0.2 * inch))
    try:
        if len(image_path) >= 2:
            img = Img(image_path[2])
            content.append(img)
        else:
            content.append(Paragraph("<b>Image not found!</b>", body_style))
    except FileNotFoundError:
        content.append(Paragraph("<b>Image not found!</b>", body_style))

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=0.6 * inch,
        rightMargin=0.6 * inch,
        topMargin=0.8 * inch,
        bottomMargin=0.6 * inch
    )
    doc.build(content, onFirstPage=draw_fancy_background, onLaterPages=draw_fancy_background)
    
def get_image_path():
    table = airtable_obj(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_CLIENT_TABLE_NAME)
    attachment_column="company_lead_magnet_images"    
    return get_dynamic_images(table,'recHwMDeb62Kgiqx1',attachment_column)
    # api = Api(AIRTABLE_API_KEY)
    # airtable = api.table(AIRTABLE_BASE_ID, AIRTABLE_CLIENT_TABLE_NAME) 
    # records = airtable.all(formula=f"{{client_id}} = '{CLIENT_NAME}'")
    # if records:
    #     return str(records[0]['fields']['company_lead_magnet_images'])
    # else:
    #     return ''

def generate_lead_magnet_pdf(user_id):
    user_details = collect_information(user_id)
    output_pdf = "lead_magnet_personalized.pdf"
    image_path = get_image_path()
    print(f"Image path: {image_path}")
    create_pdf_with_black_background(user_details,output_pdf, image_path)
    return {"Status":"Successfull"}

if __name__=="__main__":
    output_pdf = "demo_final.pdf"
    image_path = "header.jpg"
    user_details={
        "organization_industry":"Healthcare"
    }
    create_pdf_with_black_background(user_details,output_pdf, image_path)
    print(f"PDF generated and saved at {output_pdf}")
    # print("Raw metrics:", metrics)
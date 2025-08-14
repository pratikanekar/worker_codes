from os import getenv
import ast
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import ssl
from loguru import logger
from requests import post
from schedule import Scheduler
from time import sleep

scheduler = Scheduler()

hours = getenv('HOURS')
ip = getenv('IP')
port = int(getenv('PORT'))
device_ids = ast.literal_eval(getenv('DEVICE_IDS'))
site_id = int(getenv('SITE_ID'))
data_combine_flag = getenv('COMBINE_FLAG')
send_mail_addr = ast.literal_eval(getenv('SEND_MAIL_ADDR'))


def get_data_from_api(ip, port, device_ids, site_id, start_time, end_time, m_dev_tag_code="WATER-METER",
                      duration="daily", datetime_label="Today"):
    data = []
    try:
        url = f"http://{ip}:{port}/energy_dashboard/get_all_log_book_info_by_device_id"
        for item in device_ids:
            payload = {
                "m_device_tag_code": m_dev_tag_code,
                "start_time": start_time,
                "end_time": end_time,
                "device_ids": [item],
                "site_id": site_id,
                "duration": duration,
                "datetime_label": datetime_label,
            }

            try:
                res = post(url, json=payload, timeout=10).json()
            except Exception as e:
                logger.error(f"Error fetching data from {url}: {e}")
                return data

            if len(res) > 0:
                data.append(res['data'][0])

        return data
    except Exception as e:
        logger.error(f"Error Occurred in get_data_from_api - {e}")
        return data


# Convert JSON to HTML table
def json_to_html(data):
    html = ""
    try:
        rows = ""
        for item in data:
            rows += f"""
            <tr>
                <td>{item['site_name']}</td>
                <td>{item['device_friendly_name']}</td>
                <td>{(item['time']).split('T')[0]}</td>
                <td>{item['INITIAL_FLOW']}</td>
                <td>{item['FINAL_FLOW']}</td>
                <td>{item['TOTAL_CONSUMPTION']}</td>
            </tr>
            """
        html = f"""
        <html>
        <body>
            <p>Dear Sir/Madam,</p>
            <p>Please find below the daily water consumption report:</p>
            <table border="1" cellpadding="5" cellspacing="0">
                <tr>
                    <th>Site Name</th>
                    <th>Meter Name</th>
                    <th>Date</th>
                    <th>Initial Flow (m³)</th>
                    <th>Final Flow (m³)</th>
                    <th>Daily Flow (m³)</th>
                </tr>
                {rows}
            </table>
            <p><em>This is a system-generated email. Please do not reply.</em></p>
            <p>Thanks and Regards,</p>
            <p><strong>Support Team</strong></p>
        </body>
        </html>
        """
        return html
    except Exception as e:
        logger.error(f"Error Occurred in json_to_html - {e}")
        return html


# Main task
def daily_task():
    try:
        start_time = (datetime.now().replace(hour=00, minute=00, second=00)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time = (datetime.now().replace(hour=23, minute=59, second=59)).strftime("%Y-%m-%dT%H:%M:%SZ")
        data_json = get_data_from_api(ip, port, device_ids, site_id, start_time, end_time)
        if data_combine_flag:
            html_table = json_to_html(data_json)
            for mail in send_mail_addr:
                status = send_mail(mail, html_table)
                if status:
                    logger.info(f"Successfully Email sent for Combined data To - {mail} Date - {start_time}")
        else:
            for dev in data_json:
                html_table = json_to_html([dev])
                for mail in send_mail_addr:
                    status = send_mail(mail, html_table)
                    if status:
                        logger.info(
                            f"Successfully Email sent for Device - {dev['device_friendly_name']} To - {mail} Date - {start_time}")
    except Exception as e:
        logger.error(f"Error Occurred in daily_task - {e}")


def send_mail(to_email, body):
    try:
        from_email = getenv('FROM_MAIL')
        username = getenv('MAIL_USERNAME')
        password = getenv('MAIL_PASSWORD')
        smtp_host = getenv('SMTP_HOST')
        smtp_port = int(getenv('SMTP_PORT'))
        msg = MIMEMultipart()
        msg["From"] = from_email
        msg["To"] = to_email
        msg["Subject"] = getenv('SUBJECT')

        # Here we render body into HTML format into mail
        msg.attach(MIMEText(body, "html"))

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(host=smtp_host, port=smtp_port, context=context) as server:
            server.login(username, password)
            # Here we send mail to user
            server.sendmail(from_email, to_email, msg.as_string())

        return True
    except Exception as e:
        logger.error(f'Error Occurred in send_mail - {e}')
        return False


scheduler.every().day.at(hours).do(daily_task)

if __name__ == "__main__":
    logger.info(f"Daily Email worker started...")
    while True:
        scheduler.run_pending()
        sleep(55)

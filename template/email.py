
def email_template(count=10, file_url="", logo_url="",massege_type=""):
    email_send = '''
        <!doctype html>
        <html>
          <head>
            <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
          </head>
          <body style="font-family: sans-serif;">
            <div style="display: block; margin: auto; max-width: 600px;" class="main">
              <h1 style="font-size: 18px; font-weight: bold; margin-top: 20px">
                Fieldy Bulk Import
              </h1>
              <p>Dear Admin,</p>
                <p>Total number of {count} {massege_type} data. Please
                 find the report <a href="{file_url}" target="_blank">here</a></p>
                    <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                       <tr>
                          <p>Powered by </p>
                            <td class="content-block">
                            <div style="padding:10px">
                              <img width="100px" height="36px" src="{logo_url}"/>
                              </div>
                            </td>
                        </tr>
                    </table>
            </div>
            <!-- Example of invalid for email html/css, will be detected by Mailtrap: -->
          </body>
        </html>
            '''.format(count=count,file_url= file_url , logo_url= logo_url, massege_type=massege_type)
    return email_send




def error_template(message="", traceback="", logo_url=""):
    email_send = '''
        <!doctype html>
        <html>
          <head>
            <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
          </head>
          <body style="font-family: sans-serif;">
            <div style="display: block; margin: auto; max-width: 600px;" class="main">
              <h1 style="font-size: 18px; font-weight: bold; margin-top: 20px">
                Fieldy bulk import error
              </h1>
                <div>
                <h3>{message}</h3>
                <p style="background-color:lightgrey;padding:10px">{traceback}</p>
                <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                        <tr>
                          <p>Powered by </p>
                            <td class="content-block">
                            <div style="padding:10px">
                              <img width="100px" height="36px" src="{logo_url}"/>
                              </div>
                            </td>
                        </tr>
                    </table>
                </div>
            </div>
            <!-- Example of invalid for email html/css, will be detected by Mailtrap: -->
          </body>
        </html>
            '''.format(message= message, traceback=traceback,logo_url=logo_url)
    return email_send

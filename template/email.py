
def email_template(count=10, file_url="", logo_url=""):
    email_send = '''
        <!doctype html>
        <html>
          <head>
            <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
          </head>
          <body style="font-family: sans-serif;">
            <div style="display: block; margin: auto; max-width: 600px;" class="main">
              <h1 style="font-size: 18px; font-weight: bold; margin-top: 20px">
                Fieldy Project
              </h1>
              <p>Dear Admin,</p>
                <p>Total number of {count} Skiped data. Please
                 find the report <a href="{file_url}" target="_blank">here</a></p>
            </div>
             <div class="footer">
                    <table role="presentation" border="0" cellpadding="0" cellspacing="0">

                        <tr>
                            <td class="content-block powered-by">
                                Powered by <img width="70px" height="18px" src="{logo_url}"/>
                            </td>
                        </tr>
                    </table>
                </div>
            <!-- Example of invalid for email html/css, will be detected by Mailtrap: -->

          </body>
        </html>
            '''.format(count=count,file_url= file_url , logo_url= logo_url)
    return email_send

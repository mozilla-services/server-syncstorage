<body>
   <div id="content">
   %if error:
    <strong>Wrong answer</strong>
   %endif

   <script>var RecaptchaOptions = {theme: "clean"};
   </script>
   <div style="background-color: system;">
    <form action="/misc/1.0/captcha_html" method="POST" >
      ${captcha}
    </form>
   </div>
  </div>
</body>

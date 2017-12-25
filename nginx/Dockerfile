FROM alpine:latest

MAINTAINER Azizul Hakim Shakil <Azizul_Hakim.Shakil@mailbox.tu-dresden.de>

RUN apk update

RUN apk add nginx 
RUN mkdir -p /run/nginx

COPY nginx.conf /etc/nginx/

EXPOSE 80

CMD ["nginx"]

FROM alpine:latest

MAINTAINER Azizul_Hakim_Shakil

RUN apk update

RUN apk add nginx 
RUN mkdir -p /run/nginx

COPY nginx.conf /etc/nginx/

EXPOSE 80

CMD ["nginx"]

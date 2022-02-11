FROM gitpod/workspace-full:latest
RUN pip3 install flask  && \
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl 

RUN npm install -g typescript
RUN curl -s -L "https://github.com/loft-sh/devspace/releases/latest" | sed -nE 's!.*"([^"]*devspace-linux-amd64)".*!https://github.com\1!p' | xargs -n 1 curl -L -o devspace && chmod +x devspace 
RUN sudo mv devspace /usr/local/bin 
RUN wget -q -O - https://raw.githubusercontent.com/rancher/k3d/main/install.sh | bash 
RUN curl -s -L "https://github.com/loft-sh/vcluster/releases/latest" | sed -nE 's!.*"([^"]*vcluster-linux-amd64)".*!https://github.com\1!p' | xargs -n 1 curl -L -o vcluster && chmod +x vcluster
RUN sudo mv vcluster /usr/local/bin
RUN brew install kind 
RUN brew install gh

ENV PATH="${PATH}:/home/gitpod/.pulumi/bin"
ENV PIP_USER=no

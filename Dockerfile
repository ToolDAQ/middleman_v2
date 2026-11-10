### Created by Dr. Benjamin Richards (b.richards@qmul.ac.uk)

### Download base image from repo
FROM tooldaq/mm_db:base

### Run the following commands as super user (root):
USER root

RUN cd /opt/ToolFrameworkCore && git pull && git checkout  && make clean && make -j$(nproc) \
    && cd /opt/ToolDAQFramework && git pull && make clean && make -j$(nproc)

# TODO checkout a tag
RUN cd /opt \
    && git clone https://github.com/ToolDAQ/middleman_v2.git ./middleman \
    && cd middleman/ \
    && ln -s /opt ./Dependencies \
    && . ./Setup.sh \
    && export PATH+=:/usr/pgsql-18/bin \
    && make -j$(nproc) \
    && chmod a+x /opt/middleman/run_middleman.sh

# FIXME remove pg 18 specialisation
RUN cd /opt/middleman \
    && git pull \
    && chmod a+x SetupDatabase.sh \
    && export PATH+=:/usr/pgsql-18/bin && export PGROOT=/var/lib/pgsql/18 \
    && echo 'export PATH+=:/usr/pgsql-18/bin' >> /opt/middleman/Setup.sh \
    && ./SetupDatabase.sh \
    && cp ./postgresql_lite.conf /var/lib/pgsql/18/data/postgresql.conf

RUN echo "alias cp='cp -i'" >>  /etc/rc.local ;\
    echo "cd /opt/ToolFrameworkCore && git pull && make clean && make -j4" >> /etc/rc.local ;\
    echo "cd /opt/ToolDAQFramework && git pull && make clean && make -j4" >> /etc/rc.local ;\
    echo "cd /opt/middleman && git pull && . Setup.sh && make clean && make -j4" >> /etc/rc.local ;\
    echo 'export LD_LIBRARY_PATH+=:/usr/lib:/opt/ToolFrameworkCore/lib:/opt/ToolDAQFramework/lib:/opt/boost_1_66_0/install/lib:/opt/zeromq-4.0.7/lib:/opt/libpqxx-7.10.4/install/lib' >> /etc/rc.local ;\
    echo "PGROOT=/var/lib/pgsql/18 . /opt/middleman/SetupDatabase.sh &" >> /etc/rc.local ;\
    echo "/opt/middleman/run_middleman.sh &> /dev/null &" >> /etc/rc.local ;\
    echo 'disown $!' >> /etc/rc.local ;\
    chmod +x /etc/rc.local

#ENTRYPOINT ["/bin/bash"]
#CMD ["-c",". /etc/rc.local"]
CMD /bin/bash -c '. /etc/rc.local 2>&1 | tee /web/.rclocal.log'

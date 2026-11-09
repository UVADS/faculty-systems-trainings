# Containers and Application Workflows 

Containers package application code/executables and all their dependencies needed to run them. They provide lightweight operating system-level virtualization (as opposed to hardware virtualization provided by virtual machines) and offer portability of applications across different environments. Several container projects are specifically targeted at HPC environments, addressing the unique requirements of high-performance computing systems.

When working with code it is helpful to distinguish interactive vs batch (non-interactive) workloads and development vs production phases in a software's life cycle.

![](docs/images/Containers%20and%20Application%20Workflows.png)

## Containers for Interactive JupyterLab and Batch Computing in HPC Environments

This tutorial explores how containers can provide consistent environments for interactive JupyterLab sessions and non-interactive batch computing, enabling seamless workflows from development to production in HPC environments. The examples are tailored toward use in UVA's HPC environment although the concepts apply generally.

To follow along you should

- Have some experience with Linux command-line interfaces and file organization,
- Be familiar with Jupyter notebooks,
- Have basic knowledge of Python,
- Have a basic understanding of software containers. See Container Basics and Research Computing's Container tutorials

**Tutorial:** [Containers for Interactive JupyterLab and Batch Computing in HPC Environments](https://github.com/UVADS/jlab-hpc-containers/)

## Resources

- [Docker: Get started](https://docs.docker.com/get-started/)
- [Docker CLI reference](https://docs.docker.com/reference/cli/docker/): `run`, `build`, `ps`, `exec`, `push`, and the rest
- [Dockerfile reference](https://docs.docker.com/reference/dockerfile/)
- [Docker: Building best practices](https://docs.docker.com/build/building/best-practices/)
- [uv: Using uv in Docker](https://docs.astral.sh/uv/guides/integration/docker/)
- [GitHub: Working with the Container registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)
- [OCI image annotations](https://github.com/opencontainers/image-spec/blob/main/annotations.md): the `org.opencontainers.image.*` labels
- [Rancher Desktop documentation](https://docs.rancherdesktop.io/)
- [Apptainer documentation](https://apptainer.org/docs/user/main/)
